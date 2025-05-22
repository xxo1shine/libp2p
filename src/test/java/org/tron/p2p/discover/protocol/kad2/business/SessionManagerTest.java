package org.tron.p2p.discover.protocol.kad2.business;

import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.protocol.kad2.business.session.Session;
import org.tron.p2p.discover.protocol.kad2.business.session.SessionManager;
import org.tron.p2p.discover.protocol.kad2.table.Table;
import org.tron.p2p.utils.NetUtil;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;

public class SessionManagerTest {

  @Test
  public void test() throws Exception {
    SessionManager sessionManager = new SessionManager();
    Parameter.p2pConfig = new P2pConfig();
    Parameter.p2pConfig.setNodeID(NetUtil.getNodeId());

    Table table = new Table();
    table.register(sessionManager);

    InetSocketAddress a1 = new InetSocketAddress("1.1.1.1", 1000);

    Field d = sessionManager.getClass().getDeclaredField("nodes");
    d.setAccessible(true);
    Map<InetSocketAddress, Session> map = (Map<InetSocketAddress, Session>)d.get(sessionManager);

    Node n1 = new Node(a1);
    sessionManager.addNode(n1, NetUtil.getRandomBytes(8));

    Session session = map.get(a1);
    Assert.assertFalse(session.isTableNode());

    table.addNode(n1);
    Assert.assertTrue(session.isTableNode());

    for (int i = 0; i < 6100; i++) {
      String ip = "1.2." + (i / 100) + "." + i % 256;
      sessionManager.addNode(new Node(new InetSocketAddress(ip, 1000)), NetUtil.getRandomBytes(8));
    }

    Assert.assertTrue(map.size() <= 6000);

    session = map.get(a1);
    Assert.assertTrue(session.isTableNode());

    table.dropNode(n1);
    Assert.assertFalse(session.isTableNode());

    for (int i = 0; i < 6100; i++) {
      String ip = "1.2." + (i / 100) + "." + i % 256;
      sessionManager.addNode(new Node(new InetSocketAddress(ip, 1000)), NetUtil.getRandomBytes(8));
    }

    session = map.get(a1);
    Assert.assertTrue(session == null);
  }
}
