package org.tron.p2p.discover.protocol.kad2.business;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.kad2.Pong;
import org.tron.p2p.discover.protocol.kad2.business.keepalive.KeepAliveService;
import org.tron.p2p.discover.protocol.kad2.table.Table;
import org.tron.p2p.utils.NetUtil;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Set;

public class KeepAliveServiceTest {

  @Test
  public void testNodeAdded() throws Exception {
    KeepAliveService keepAliveService = new KeepAliveService(null, null, null);

    InetSocketAddress address = new InetSocketAddress("1.1.1.1", 1000);
    init(keepAliveService, address);

    assertResult(keepAliveService, 0, 1);
  }

  @Test
  public void testNodeRemoved() throws Exception {
    KeepAliveService keepAliveService = new KeepAliveService(null, null, null);
    InetSocketAddress address = new InetSocketAddress("1.1.1.1", 1000);
    init(keepAliveService, address);
    keepAliveService.nodeRemoved(new Node(address));
    assertResult(keepAliveService, 0, 0);
  }

  @Test
  public void testFailFindNodes() throws Exception {
    KeepAliveService keepAliveService = new KeepAliveService(null, null, null);
    InetSocketAddress address = new InetSocketAddress("1.1.1.1", 1000);
    init(keepAliveService, address);
    keepAliveService.failFindNodes(address);
    assertResult(keepAliveService, 1, 0);
  }

  @Test
  public void testHandleResponse() throws Exception {
    KeepAliveService keepAliveService = new KeepAliveService(null, null, null);
    InetSocketAddress a1 = new InetSocketAddress("192.168.1.1", 1000);
    init(keepAliveService, a1);
    keepAliveService.failFindNodes(a1);
    assertResult(keepAliveService, 1, 0);

    InetSocketAddress a2 = new InetSocketAddress("192.168.1.2", 1000);
    Pong pong = new Pong(ByteString.copyFrom(new byte[8]), a2);
    keepAliveService.handleResponse(pong, a2);
    assertResult(keepAliveService, 1, 0);

    pong = new Pong(ByteString.copyFrom(new byte[8]), a1);
    keepAliveService.handleResponse(pong, a1);
    assertResult(keepAliveService, 0, 1);
  }

  @Test
  public void testTimeout() throws Exception {
    Parameter.p2pConfig = new P2pConfig();
    Parameter.p2pConfig.setNodeID(NetUtil.getNodeId());

    Table table = new Table();
    KeepAliveService keepAliveService = new KeepAliveService(null, null, table);
    table.register(keepAliveService);

    InetSocketAddress a1 = new InetSocketAddress("192.168.1.1", 1000);
    Node node = new Node(a1);
    table.addNode(node);

    assertResult(keepAliveService, 0, 1);

    Pong pong = new Pong(ByteString.copyFrom(new byte[8]), a1);
    keepAliveService.handleResponse(pong, a1);
    keepAliveService.handleResponse(pong, a1);

    keepAliveService.timeout(a1);
    assertResult(keepAliveService, 1, 0);

    keepAliveService.timeout(a1);
    assertResult(keepAliveService, 0, 0);
  }

  private void init(KeepAliveService keepAliveService, InetSocketAddress address) {
    Node node = new Node(address);
    keepAliveService.nodeAdded(node);
  }

  private void assertResult(KeepAliveService keepAliveService, int fastSize, int slowSize) throws Exception {
    Field declaredField = keepAliveService.getClass().getDeclaredField("fastNodes");
    declaredField.setAccessible(true);
    Set<InetSocketAddress> fastNodes = (Set<InetSocketAddress>)declaredField.get(keepAliveService);

    declaredField = keepAliveService.getClass().getDeclaredField("slowNodes");
    declaredField.setAccessible(true);
    Set<InetSocketAddress> slowNodes = (Set<InetSocketAddress>)declaredField.get(keepAliveService);

    Assert.assertEquals(fastNodes.size(), fastSize);
    Assert.assertEquals(slowNodes.size(), slowSize);
  }
}
