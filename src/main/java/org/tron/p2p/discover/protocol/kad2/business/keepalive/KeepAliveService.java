package org.tron.p2p.discover.protocol.kad2.business.keepalive;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.kad2.Ping;
import org.tron.p2p.discover.message.kad2.Pong;
import org.tron.p2p.discover.protocol.kad2.business.call.CallBack;
import org.tron.p2p.discover.protocol.kad2.business.call.CallService;
import org.tron.p2p.discover.protocol.kad2.business.ip.IPTracker;
import org.tron.p2p.discover.protocol.kad2.business.session.Session;
import org.tron.p2p.discover.protocol.kad2.business.session.SessionManager;
import org.tron.p2p.discover.protocol.kad2.table.NodeEventListener;
import org.tron.p2p.discover.protocol.kad2.table.Table;
import org.tron.p2p.utils.NetUtil;

import static org.tron.p2p.base.Constant.REQ_ID_SIZE;

@Slf4j(topic = "net")
public class KeepAliveService implements CallBack, NodeEventListener {

  private final static int FAST_SCHEDULE_TIME = 5;
  private final static int SLOW_SCHEDULE_TIME = FAST_SCHEDULE_TIME * 3;

  private final Set<InetSocketAddress> fastNodes = new HashSet<>();
  private final Set<InetSocketAddress> slowNodes = new HashSet<>();
  private final Map<InetSocketAddress, TableNode> tableNodes = new ConcurrentHashMap<>();

  private final ScheduledExecutorService fastTimer = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder().namingPattern("fastTimer").build());;
  private final ScheduledExecutorService slowTimer = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder().namingPattern("slowTimer").build());

  private final CallService callService;
  private final SessionManager sessionManager;
  private final Table table;

  public KeepAliveService(CallService callService,
                          SessionManager sessionManager,
                          Table table) {
    this.callService = callService;
    this.sessionManager = sessionManager;
    this.table = table;
  }

  public void init () {
    fastTimer.scheduleWithFixedDelay(() -> {
      try {
        doLivenessCheck(fastNodes);
      } catch (Exception e) {
        log.error("KAD2 Do fast liveness check fail.", e);
      }
    }, 1, FAST_SCHEDULE_TIME, TimeUnit.SECONDS);

    slowTimer.scheduleWithFixedDelay(() -> {
      try {
        doLivenessCheck(slowNodes);
      } catch (Exception e) {
        log.error("KAD2 Do slow liveness check fail.", e);
      }
    }, 1, SLOW_SCHEDULE_TIME, TimeUnit.SECONDS);

    log.debug("KAD2 KeepAliveService start.");
  }

  public void close () {
    fastTimer.shutdown();
    slowTimer.shutdown();
    log.debug("KAD2 KeepAliveService close.");
  }

  private void doLivenessCheck(Set<InetSocketAddress> nodes) {
    InetSocketAddress address = getNode(nodes);
    if (address == null) {
      return;
    }

    ByteString key = null;
    Session session = sessionManager.getSession(address);
    if (session != null) {
      key = session.getKey();
    }
    Ping ping = new Ping(NetUtil.getRandomBytes(REQ_ID_SIZE), key);
    callService.sendCall(address, ping, this);
  }

  private InetSocketAddress getNode(Set<InetSocketAddress> nodes) {
    if (nodes.isEmpty()) {
      return null;
    }
    List<InetSocketAddress> list = new ArrayList<>(nodes);
    if (list.isEmpty()) {
      return null;
    }
    return list.get(new Random().nextInt(list.size()));
  }

  public synchronized void failFindNodes(InetSocketAddress address) {
    if (!tableNodes.containsKey(address) || fastNodes.contains(address)) {
      return;
    }
    slowNodes.remove(address);
    fastNodes.add(address);
    log.debug("KAD2 KeepAliveService failFindNodes {}, slow: {}, fast: {}", address, slowNodes.size(), fastNodes.size());
  }

  private synchronized void move(InetSocketAddress address,
                                 Set<InetSocketAddress> src,
                                 Set<InetSocketAddress> des) {
    if (des.contains(address)){
      return;
    }
    src.remove(address);
    des.add(address);
    log.debug("KAD2 KeepAliveService move {}, slow: {}, fast: {}", address, slowNodes.size(), fastNodes.size());
  }

  @Override
  public synchronized void handleResponse(Message message, InetSocketAddress address) {
    log.debug("KAD2 KeepAliveService process response from {}", address);
    Pong pong = (Pong) message;
    IPTracker.addStatement(address.getAddress(), pong.getToIp());
    TableNode tableNode = tableNodes.get(address);
    if (tableNode != null) {
      tableNode.setLivenessChecks(tableNode.getLivenessChecks() + 1);
      move(address, fastNodes, slowNodes);
    }
  }

  @Override
  public synchronized void timeout(InetSocketAddress address) {
    log.debug("KAD2 KeepAliveService process timeout from {}", address);
    TableNode tableNode = tableNodes.get(address);
    if (tableNode == null) {
      log.debug("KAD2 KeepAliveService not find node {}", address);
      return;
    }

    tableNode.setLivenessChecks(tableNode.getLivenessChecks() / 3);
    if (tableNode.getLivenessChecks() == 0) {
      table.dropNode(tableNode.getNode());
      return;
    }

    move(address, slowNodes, fastNodes);
  }

  @Override
  public synchronized void nodeAdded(Node node) {
    log.debug("KAD2 KeepAliveService process nodeAdded from {}", node.getInetSocketAddress());
    InetSocketAddress address = node.getInetSocketAddress();
    if (tableNodes.get(address) != null) {
      log.debug("KAD2 KeepAliveService node {} is existed", address);
      return;
    }
    tableNodes.put(address, new TableNode(node));
    slowNodes.add(address);
  }

  @Override
  public synchronized void nodeRemoved(Node node) {
    log.debug("KAD2 KeepAliveService process nodeRemoved from {}", node.getInetSocketAddress());
    InetSocketAddress address = node.getInetSocketAddress();
    tableNodes.remove(address);
    slowNodes.remove(address);
    fastNodes.remove(address);
  }
}
