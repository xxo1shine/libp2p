package org.tron.p2p.discover.protocol.kad2.business.session;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.protocol.kad2.table.NodeEventListener;

import static org.tron.p2p.base.Constant.MAX_NODES;

@Slf4j(topic = "net")
public class SessionManager implements NodeEventListener {

  private static final int NODES_TRIM_THRESHOLD = MAX_NODES * 2;

  private final Map<InetSocketAddress, Session> nodes = new ConcurrentHashMap<>();

  private void trim() {
    if (nodes.size() > NODES_TRIM_THRESHOLD) {
      nodes.forEach((k, v) -> {
        if (!v.getNode().isConnectible()) {
          nodes.remove(k);
        }
      });
    }

    if (nodes.size() > NODES_TRIM_THRESHOLD) {
      List<Session> sorted = new ArrayList<>(nodes.values());
      sorted.sort(Comparator.comparingLong(Session::getHandshakeTime));
      for (Session session : sorted) {
        if (!session.isTableNode()) {
          nodes.remove(session.getNode().getInetSocketAddress());
          if (nodes.size() <= MAX_NODES) {
            break;
          }
        }
      }
    }
  }

  public List<Node> getConnectableNodes() {
    return nodes.values().stream()
            .map(Session::getNode)
            .filter(Node::isConnectible)
            .collect(Collectors.toList());
  }

  public List<Node> getAllNodes() {
    return nodes.values().stream()
            .map(Session::getNode)
            .collect(Collectors.toList());
  }

  public void addNode(Node node, ByteString sessionKey) {
    log.debug("KAD2 Add session {}, {}, session-size {}", node.getInetSocketAddress(), sessionKey, nodes.size());
    trim();
    nodes.put(node.getInetSocketAddress(), new Session(node, sessionKey));
  }

  public Session getSession(InetSocketAddress address) {
    return nodes.get(address);
  }

  @Override
  public void nodeAdded(Node node) {
    log.debug("KAD2 SessionManager process nodeAdded from {}, session-size {}", node.getInetSocketAddress(), nodes.size());
    Session session = nodes.get(node.getInetSocketAddress());
    if (session != null) {
      session.setTableNode(true);
    }
  }

  @Override
  public void nodeRemoved(Node node) {
    log.debug("KAD2 SessionManager process nodeRemoved from {}, session-size {}", node.getInetSocketAddress(), nodes.size());
    Session session = nodes.get(node.getInetSocketAddress());
    if (session != null) {
      session.setTableNode(false);
    }
  }
}
