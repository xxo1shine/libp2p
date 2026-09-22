package org.tron.p2p.discover.protocol.kad;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.kad.FindNodeMessage;
import org.tron.p2p.discover.message.kad.NeighborsMessage;
import org.tron.p2p.discover.message.kad.PingMessage;
import org.tron.p2p.discover.message.kad.PongMessage;
import org.tron.p2p.discover.protocol.kad.table.KademliaOptions;
import org.tron.p2p.discover.socket.UdpEvent;
import org.tron.p2p.utils.NetUtil;

/** Processes census responses and visits each discovered node's K buckets. */
@Slf4j(topic = "net")
public class NeighborsHandler {

  private static final int SLOW_RETRY_AFTER_FAILURES = 5;
  private static final long FIND_NODE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
  private static final long RETRY_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long SLOW_RETRY_MS = TimeUnit.HOURS.toMillis(1);
  private static final long COMPLETED_SCAN_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  private static final long RESPONSE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private final KadService kadService;
  private final Map<InetSocketAddress, PendingNode> nodes = new ConcurrentHashMap<>();
  private final AtomicBoolean started = new AtomicBoolean();
  private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(
      BasicThreadFactory.builder().namingPattern("neighborsWorker").build());

  public NeighborsHandler(KadService kadService) {
    this.kadService = kadService;
  }

  public void start(List<Node> seeds) {
    if (!started.compareAndSet(false, true)) {
      return;
    }
    for (Node seed : seeds) {
      addNode(seed);
      kadService.sendOutbound(new UdpEvent(
          new PingMessage(kadService.getPublicHomeNode(), seed),
          seed.getPreferInetSocketAddress()));
    }
    worker.scheduleWithFixedDelay(this::sendNext, 0, KademliaOptions.WAIT_TIME,
        TimeUnit.MILLISECONDS);
  }

  public void close() {
    worker.shutdownNow();
  }

  public List<Node> getNodes() {
    List<Node> result = new ArrayList<>();
    for (PendingNode pending : nodes.values()) {
      synchronized (pending) {
        result.add(new Node(pending.id, pending.ip, null,
            pending.port, pending.bindPort));
      }
    }
    return result;
  }

  public List<PendingNode> getPendingNodes() {
    List<PendingNode> result = new ArrayList<>();
    for (PendingNode pending : nodes.values()) {
      synchronized (pending) {
        PendingNode snapshot = new PendingNode(pending.id, pending.ip,
            pending.port, pending.bindPort);
        snapshot.k = pending.k;
        snapshot.cycle = pending.cycle;
        snapshot.findNodeFailures = pending.findNodeFailures;
        snapshot.requestTimestamp = pending.requestTimestamp;
        snapshot.waiting = pending.waiting;
        result.add(snapshot);
      }
    }
    return result;
  }

  public void handle(NeighborsMessage message, InetSocketAddress sender) {
    if (!(sender.getAddress() instanceof Inet4Address)) {
      return;
    }
    PendingNode pending = nodes.get(sender);
    if (pending != null) {
      synchronized (pending) {
        if (pending.waiting
            && System.currentTimeMillis() - pending.requestTimestamp < FIND_NODE_TIMEOUT_MS) {
          pending.waiting = false;
          pending.findNodeFailures = 0;
          pending.setId(message.getFrom().getId());
          pending.k++;
          if (pending.k == KademliaOptions.BINS) {
            pending.k = 0;
            pending.cycle++;
          }
          pending.requestTimestamp = System.currentTimeMillis();
        }
      }
    }
    for (Node node : message.getNodes()) {
      addNode(node);
    }
  }

  public void handlePing(PingMessage message, InetSocketAddress sender) {
    Node node = message.getFrom();
    node.setPort(sender.getPort());
    addNode(node);
    kadService.sendOutbound(new UdpEvent(
        new PongMessage(kadService.getPublicHomeNode()), sender));
  }

  private void addNode(Node node) {
    if (!NetUtil.validNode(node) || !NetUtil.validIpV4(node.getHostV4())) {
      return;
    }
    PendingNode pending = new PendingNode(node.getId(), node.getHostV4(),
        node.getPort(), node.getBindPort());
    pending.requestTimestamp = System.currentTimeMillis();
    if (pending.address().equals(kadService.getPublicHomeNode().getInetSocketAddressV4())) {
      return;
    }
    PendingNode existing = nodes.putIfAbsent(pending.address(), pending);
    if (existing != null) {
      synchronized (existing) {
        existing.setId(node.getId());
        existing.setPort(node.getPort());
      }
    }
  }

  private void sendNext() {
    for (PendingNode pending : nodes.values()) {
      sendNode(pending);
    }
  }

  private void sendNode(PendingNode pending) {
    synchronized (pending) {
      if (pending.waiting) {
        if (System.currentTimeMillis() - pending.requestTimestamp >= FIND_NODE_TIMEOUT_MS) {
          pending.waiting = false;
          onFailure(pending);
        }
        return;
      }
      long interval = pending.findNodeFailures >= SLOW_RETRY_AFTER_FAILURES
          ? SLOW_RETRY_MS : pending.findNodeFailures > 0 ? RETRY_MS
          : pending.cycle > 0 ? COMPLETED_SCAN_INTERVAL_MS : RESPONSE_INTERVAL_MS;
      if (pending.port != pending.bindPort
          || System.currentTimeMillis() - pending.requestTimestamp < interval) {
        return;
      }
      try {
        byte[] target = targetForBucket(pending.id, pending.k,
            ThreadLocalRandom.current());
        FindNodeMessage message = new FindNodeMessage(kadService.getPublicHomeNode(), target);
        long requestTime = System.currentTimeMillis();
        pending.requestTimestamp = requestTime;
        pending.waiting = true;
        InetSocketAddress address = pending.address();
        kadService.sendOutbound(new UdpEvent(message, address));
      } catch (Exception e) {
        pending.waiting = false;
        onFailure(pending);
        log.warn("Failed to send census findnode to {}", pending.address(), e);
        return;
      }
    }
    try {
      TimeUnit.MILLISECONDS.sleep(1);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void onFailure(PendingNode pending) {
    pending.findNodeFailures++;
    pending.requestTimestamp = System.currentTimeMillis();
  }

  private static byte[] targetForBucket(byte[] nodeId, int k, Random random) {
    byte[] target = new byte[nodeId.length];
    random.nextBytes(target);
    int commonBits = KademliaOptions.BINS - k - 1;
    int fullBytes = commonBits / 8;
    int remainingBits = commonBits % 8;
    System.arraycopy(nodeId, 0, target, 0, fullBytes);
    int prefixMask = (0xff << (8 - remainingBits)) & 0xff;
    int flipMask = 0x80 >>> remainingBits;
    target[fullBytes] = (byte) ((target[fullBytes] & ~(prefixMask | flipMask))
        | (nodeId[fullBytes] & prefixMask)
        | (~nodeId[fullBytes] & flipMask));
    return target;
  }

  public static final class PendingNode {
    private byte[] id;
    @Getter
    @Setter
    private String ip;
    @Getter
    @Setter
    private int port;
    @Getter
    @Setter
    private int bindPort;
    @Getter
    @Setter
    private int k;
    @Getter
    @Setter
    private int cycle;
    @Getter
    @Setter
    private int findNodeFailures;
    private long requestTimestamp;
    private boolean waiting;

    private PendingNode(byte[] id, String ip, int port, int bindPort) {
      setId(id);
      this.ip = ip;
      this.port = port;
      this.bindPort = bindPort;
    }

    public void setId(byte[] id) {
      this.id = id == null ? null : id.clone();
    }

    private InetSocketAddress address() {
      return new InetSocketAddress(ip, bindPort);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof PendingNode)) {
        return false;
      }
      PendingNode that = (PendingNode) other;
      return bindPort == that.bindPort && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ip, bindPort);
    }
  }
}
