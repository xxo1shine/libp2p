package org.tron.p2p.discover.protocol.kad2.business.lookup;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.kad2.FindNodes;
import org.tron.p2p.discover.message.kad2.Nodes;
import org.tron.p2p.discover.protocol.kad2.business.call.CallBack;
import org.tron.p2p.discover.protocol.kad2.business.call.CallService;
import org.tron.p2p.discover.protocol.kad2.business.handshake.HandshakeService;
import org.tron.p2p.discover.protocol.kad2.business.keepalive.KeepAliveService;
import org.tron.p2p.discover.protocol.kad2.business.session.Session;
import org.tron.p2p.discover.protocol.kad2.business.session.SessionManager;
import org.tron.p2p.discover.protocol.kad2.table.Kad2Options;
import org.tron.p2p.discover.protocol.kad2.table.Table;
import org.tron.p2p.utils.NetUtil;

import static org.tron.p2p.base.Constant.REQ_ID_SIZE;

@Slf4j(topic = "net")
public class LookupTask implements CallBack {

  private final ScheduledExecutorService discoverer = Executors.newSingleThreadScheduledExecutor(
      new BasicThreadFactory.Builder().namingPattern("discoverTask").build());

  private final SessionManager sessionManager;
  private final CallService callService;
  private final HandshakeService handshakeService;
  private final KeepAliveService keepAliveService;
  private final Table table;

  private int loopNum = 0;
  private byte[] nodeId;

  public LookupTask(SessionManager sessionManager,
                    CallService callService,
                    HandshakeService handshakeService,
                    KeepAliveService keepAliveService,
                    Table table) {
    this.sessionManager = sessionManager;
    this.callService = callService;
    this.handshakeService = handshakeService;
    this.keepAliveService = keepAliveService;
    this.table = table;
  }

  public void init() {
    discoverer.scheduleWithFixedDelay(() -> {
      try {
        loopNum++;
        if (loopNum % Kad2Options.MAX_LOOP_NUM == 0) {
          loopNum = 0;
          nodeId = Parameter.p2pConfig.getNodeID();
        } else {
          nodeId = NetUtil.getNodeId();
        }
        discover(nodeId, 0, new ArrayList<>());
      } catch (Exception e) {
        log.error("KAD2 DiscoverTask fails to be executed", e);
      }
    }, 1, Kad2Options.DISCOVER_CYCLE, TimeUnit.MILLISECONDS);
    log.debug("KAD2 DiscoverTask started");
  }

  private void discover(byte[] nodeId, int round, List<Node> prevTriedNodes) {
    List<Node> closest = table.getClosestNodes(nodeId);
    List<Node> tried = new ArrayList<>();
    for (Node n : closest) {
      if (!tried.contains(n) && !prevTriedNodes.contains(n)) {
        try {
          Session session = sessionManager.getSession(n.getInetSocketAddress());
          if (session == null) {
            handshakeService.initHandshake(n.getInetSocketAddress());
          } else {
            ByteString reqId = NetUtil.getRandomBytes(REQ_ID_SIZE);
            ByteString sessionKey = session.getKey();
            FindNodes findNodes = new FindNodes(reqId, sessionKey, ByteString.copyFrom(nodeId));
            callService.sendCall(n.getInetSocketAddress(), findNodes, this);
          }
          tried.add(n);
        } catch (Exception e) {
          log.error("KAD2 Unexpected Exception occurred while sending FindNodeMessage", e);
        }
      }

      if (tried.size() == Kad2Options.ALPHA) {
        break;
      }
    }

    try {
      Thread.sleep(Kad2Options.WAIT_TIME);
    } catch (InterruptedException e) {
      log.warn("KAD2 Discover task interrupted");
      Thread.currentThread().interrupt();
    }

    if (tried.isEmpty()) {
      return;
    }

    if (++round == Kad2Options.MAX_STEPS) {
      return;
    }
    tried.addAll(prevTriedNodes);
    discover(nodeId, round, tried);
  }

  public void close() {
    discoverer.shutdownNow();
  }

  @Override
  public void handleResponse(Message message, InetSocketAddress address) {
    log.debug("KAD2 LookupTask process response from {}", address);
    Nodes nodes = (Nodes) message;
    for(Node n: nodes.getNodes()) {
      handshakeService.initHandshake(n.getInetSocketAddress());
    }
  }

  @Override
  public void timeout(InetSocketAddress address) {
    log.debug("KAD2 LookupTask process timeout from {}", address);
    keepAliveService.failFindNodes(address);
  }
}
