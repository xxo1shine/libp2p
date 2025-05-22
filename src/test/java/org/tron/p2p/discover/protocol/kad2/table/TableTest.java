package org.tron.p2p.discover.protocol.kad2.table;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.utils.NetUtil;

import static org.tron.p2p.base.Constant.NODE_ID_SIZE;
import static org.tron.p2p.discover.protocol.kad2.table.Kad2Options.BUCKET_SIZE;
import static org.tron.p2p.discover.protocol.kad2.table.Kad2Options.MAX_REPLACEMENTS;

public class TableTest implements NodeEventListener {
  private static final AtomicInteger nodeAddCnt = new AtomicInteger(0);
  private static final AtomicInteger nodeRemoveCnt = new AtomicInteger(0);
  private static final TableTest tableTest = new TableTest();

  @Test
  public void test() {
    Parameter.p2pConfig = new P2pConfig();

    Table table = new Table();

    table.register(tableTest);

    Node node = new Node(new InetSocketAddress("127.0.0.1", 1000));
    table.addNode(node);
    List<Node> nodes = table.getTableNodes();
    Assert.assertEquals(1, nodes.size());
    Assert.assertEquals(node, nodes.get(0));
    Assert.assertEquals(1, nodeAddCnt.get());

    table.dropNode(node);
    nodes = table.getTableNodes();
    Assert.assertEquals(0, nodes.size());
    Assert.assertEquals(1, nodeRemoveCnt.get());

    int size = BUCKET_SIZE + MAX_REPLACEMENTS + 10;
    byte[] nodeId = NetUtil.getRandomBytes(NODE_ID_SIZE).toByteArray();
    for (int i = 1; i <= size; i++) {
      nodeId[NODE_ID_SIZE - 1] = (byte)i;
      Node n = new Node(new InetSocketAddress("127.0.0." + i, 1));
      n.setId(nodeId);
      table.addNode(n);
    }

    nodes = table.getTableNodes();
    Assert.assertEquals(BUCKET_SIZE, nodes.size());
    Assert.assertEquals(BUCKET_SIZE + 1, nodeAddCnt.get());
    Assert.assertEquals(1, nodeRemoveCnt.get());

    nodes.forEach(table::dropNode);
    nodes = table.getTableNodes();
    Assert.assertEquals(MAX_REPLACEMENTS, nodes.size());
    Assert.assertEquals(BUCKET_SIZE + MAX_REPLACEMENTS + 1, nodeAddCnt.get());
    Assert.assertEquals(BUCKET_SIZE + 1, nodeRemoveCnt.get());
  }

  @Override
  public void nodeAdded(Node node) {
    nodeAddCnt.incrementAndGet();
  }

  @Override
  public void nodeRemoved(Node node) {
    nodeRemoveCnt.incrementAndGet();
  }
}
