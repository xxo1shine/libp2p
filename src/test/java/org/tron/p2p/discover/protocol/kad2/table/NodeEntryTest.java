package org.tron.p2p.discover.protocol.kad2.table;

import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.utils.NetUtil;

import java.net.InetSocketAddress;

public class NodeEntryTest {
  @Test
  public void testDistance() {
    byte[] id1 = new byte[64];
    byte[] id2 = new byte[64];
    id2[0] = (byte) 0xff;

    int distance = NodeEntry.distance(id1, id2);
    Assert.assertEquals(distance, 255);

    id2[0] = (byte) 0x1;
    distance = NodeEntry.distance(id1, id2);
    Assert.assertEquals(distance, 248);
  }

  @Test
  public void testEquals() {
    Parameter.p2pConfig = new P2pConfig();
    Parameter.p2pConfig.setNodeID(NetUtil.getNodeId());

    InetSocketAddress address1 = new InetSocketAddress("127.0.0.1", 1000);
    Node n1 = new Node(address1);

    InetSocketAddress address2 = new InetSocketAddress("127.0.0.2", 1000);
    Node n2 = new Node(address2);

    InetSocketAddress address3 = new InetSocketAddress("127.0.0.2", 2000);
    Node n3 = new Node(address3);

    NodeEntry entry1 = new NodeEntry(n1);
    NodeEntry entry2 = new NodeEntry(n2);
    NodeEntry entry3 = new NodeEntry(n3);

    Assert.assertFalse(entry1.equals(entry2));
    Assert.assertTrue(entry2.equals(entry3));
  }
}
