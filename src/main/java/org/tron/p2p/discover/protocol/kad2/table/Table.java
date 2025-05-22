package org.tron.p2p.discover.protocol.kad2.table;

import com.google.protobuf.ByteString;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.utils.NetUtil;

@Slf4j(topic = "net")
public class Table {
  private final NodeBucket[] buckets;
  private final Map<InetAddress, NodeEntry> ipMap = new HashMap<>();
  private final Map<ByteString, NodeEntry> idMap = new HashMap<>();
  @Getter
  private final List<NodeEventListener> listeners = new ArrayList<>();

  public Table() {
    buckets = new NodeBucket[Kad2Options.BINS];
    for (int i = 0; i < Kad2Options.BINS; i++) {
      buckets[i] = new NodeBucket(this);
    }
  }

  public void register(NodeEventListener listener) {
    listeners.add(listener);
  }

  public synchronized void addNode(Node n) {
    if (Arrays.equals(n.getId(), Parameter.p2pConfig.getNodeID())
    || idMap.get(ByteString.copyFrom(n.getId())) != null) {
      return;
    }

    InetAddress address = n.getInetSocketAddress().getAddress();
    NodeEntry entry = ipMap.get(address);
    if (entry != null) {
      remove(entry);
    }

    NodeEntry e = new NodeEntry(n);
    idMap.put(ByteString.copyFrom(n.getId()), e);
    ipMap.put(address, e);
    buckets[e.getDistance()].add(e);
    log.debug("KAD2 Add node {}, k-size {}, total {}",
      n.getInetSocketAddress(), getTableNodes().size(), ipMap.size());
  }

  public synchronized void dropNode(Node n) {
    remove(new NodeEntry(n));
    log.debug("KAD2 Remove node {}, k-size {}, total {}",
      n.getInetSocketAddress(), getTableNodes().size(), ipMap.size());
  }

  public synchronized List<Node> getTableNodes() {
    return ipMap.values().stream()
            .filter(NodeEntry::isInTable)
            .map(e -> (Node)e.getNode().clone())
            .collect(Collectors.toList());
//    List<Node> nodes = new ArrayList<>();
//    for (int i = 0; i < Kad2Options.BINS; i++) {
//      buckets[i] = new NodeBucket(this);
//      nodes.addAll(buckets[i].getEntries());
//    }
  }

  private void remove(NodeEntry entry) {
    idMap.remove(ByteString.copyFrom(entry.getNode().getId()));
    ipMap.remove(entry.getEntryId());
    buckets[entry.getDistance()].remove(entry);
  }

  public synchronized List<Node> getClosestNodes(byte[] targetId) {
    List<Node> closestNodes = getTableNodes();
    closestNodes.sort(new DistanceComparator(targetId));
    if (closestNodes.size() > Kad2Options.BUCKET_SIZE) {
      closestNodes = closestNodes.subList(0, Kad2Options.BUCKET_SIZE);
    }
    return closestNodes;
  }

  public synchronized List<Node> getClosestNodes(byte[] targetId, InetAddress address) {
    List<Node> closestNodes = getTableNodes().stream()
            .filter(n -> isAvailable(address, n.getInetSocketAddress().getAddress()))
            .sorted(new DistanceComparator(targetId))
            .collect(Collectors.toList());
    if (closestNodes.size() > Kad2Options.BUCKET_SIZE) {
      closestNodes = closestNodes.subList(0, Kad2Options.BUCKET_SIZE);
    }
    return closestNodes;
  }

  private boolean isAvailable(InetAddress src, InetAddress dec) {
    if (src instanceof Inet4Address && dec instanceof Inet6Address
       || src instanceof Inet6Address && dec instanceof Inet4Address) {
      return false;
    }
    if (src.isLoopbackAddress() || NetUtil.isPrivateIp(src)) {
      return true;
    }
    return !dec.isLoopbackAddress() && !NetUtil.isPrivateIp(dec);
  }
}
