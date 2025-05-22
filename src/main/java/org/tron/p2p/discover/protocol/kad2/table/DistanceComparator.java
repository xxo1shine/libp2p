package org.tron.p2p.discover.protocol.kad2.table;

import java.util.Comparator;
import org.tron.p2p.discover.Node;

public class DistanceComparator implements Comparator<Node> {
  private final byte[] targetId;

  DistanceComparator(byte[] targetId) {
    this.targetId = targetId;
  }

  @Override
  public int compare(Node e1, Node e2) {
    int d1 = NodeEntry.distance(targetId, e1.getId());
    int d2 = NodeEntry.distance(targetId, e2.getId());

    return Integer.compare(d1, d2);
  }
}
