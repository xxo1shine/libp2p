package org.tron.p2p.discover.protocol.kad2.business.keepalive;

import lombok.Data;
import org.tron.p2p.discover.Node;

@Data
public class TableNode {
  private Node node;
  private int livenessChecks;

  public TableNode(Node node) {
    this.node = node;
    this.livenessChecks = 2;
  }
}
