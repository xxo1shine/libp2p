package org.tron.p2p.discover.protocol.kad2.table;

import java.net.InetAddress;
import lombok.Getter;
import lombok.Setter;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;

public class NodeEntry {
  @Getter
  private final Node node;
  @Getter
  private final InetAddress entryId;
  @Getter
  private final int distance;
  @Getter
  @Setter
  private volatile boolean isInTable;

  public NodeEntry(Node n) {
    this.node = n;
    entryId = n.getInetSocketAddress().getAddress();
    distance = distance(Parameter.p2pConfig.getNodeID(), n.getId());
  }

  public static int distance(byte[] ownerId, byte[] targetId) {
    byte[] hash = new byte[Math.min(targetId.length, ownerId.length)];
    for (int i = 0; i < hash.length; i++) {
      hash[i] = (byte) (targetId[i] ^ ownerId[i]);
    }

    int d = Kad2Options.BINS;

    for (byte b : hash) {
      if (b == 0) {
        d -= 8;
      } else {
        int count = 0;
        for (int i = 7; i >= 0; i--) {
          boolean a = ((b & 0xff) & (1 << i)) == 0;
          if (a) {
            count++;
          } else {
            break;
          }
        }
        d -= count;
        break;
      }
    }
    return Math.max(d - 1, 0);
  }

  @Override
  public boolean equals(Object o) {
    boolean ret = false;

    if (o != null && this.getClass() == o.getClass()) {
      NodeEntry e = (NodeEntry) o;
      ret = this.getEntryId().equals(e.getEntryId());
    }

    return ret;
  }

  @Override
  public int hashCode() {
    return this.entryId.hashCode();
  }
}
