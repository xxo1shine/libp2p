package org.tron.p2p.discover.protocol.kad2.table;

import org.tron.p2p.discover.Node;

public interface NodeEventListener {
  void nodeAdded(Node node);
  void nodeRemoved(Node node);
}