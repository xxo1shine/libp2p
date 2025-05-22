package org.tron.p2p.discover.protocol.kad2;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.P2pService;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.DiscoverService;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.NodeManager;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.kad.FindNodeMessage;
import org.tron.p2p.discover.message.kad.PingMessage;
import org.tron.p2p.discover.socket.DiscoverServer;
import org.tron.p2p.discover.socket.UdpEvent;
import org.tron.p2p.utils.NetUtil;

public class Kad2ServiceTest implements DiscoverService {

  private Consumer<UdpEvent> messageSender;

  public int cnt = 0;

  @Override
  public void init() {

  }

  @Override
  public void close() {

  }

  @Override
  public List<Node> getConnectableNodes() {
    return Collections.emptyList();
  }

  @Override
  public List<Node> getTableNodes() {
    return Collections.emptyList();
  }

  @Override
  public List<Node> getAllNodes() {
    return Collections.emptyList();
  }

  @Override
  public Node getPublicHomeNode() {
    return null;
  }

  @Override
  public void channelActivated() {

  }

  @Override
  public void handleEvent(UdpEvent event) {
    cnt++;
    System.out.println("### cnt" + cnt);
  }

  @Override
  public void setMessageSender(Consumer<UdpEvent> messageSender) {
    this.messageSender = messageSender;
  }

  public void send(InetSocketAddress address, Message message) {
    messageSender.accept(new UdpEvent(message, address));
  }

  public static void main(String[] args) throws Exception {
    P2pConfig config = new P2pConfig();
    config.setNetworkId(11111);
    P2pService service = new P2pService();
    config.setPort(18888);
    service.start(config);


    InetSocketAddress address = new InetSocketAddress("52.2.118.138", 18801);
    Node node = new Node(address);
    FindNodeMessage findNodeMessage = new FindNodeMessage(node, NetUtil.getNodeId());
    while (true) {
//      NodeManager.getDiscoverService().handleEvent(new UdpEvent(findNodeMessage, address));
      Thread.sleep(5000);
    }


//    Thread.sleep(5000);
//
//
//    Parameter.p2pConfig.setPort(18889);
//    Kad2ServiceTest kad2ServiceTest = new Kad2ServiceTest();
//    DiscoverServer server = new DiscoverServer();
//    server.init(kad2ServiceTest);
//
//    InetSocketAddress sever = new InetSocketAddress("52.2.118.138", 18801);
//    Node sNode = new Node(sever);
//
//    InetSocketAddress client = new InetSocketAddress("127.0.0.1", 18889);
//    Node cNode = new Node(client);
//
//    kad2ServiceTest.send(sever, new PingMessage(sNode, cNode));
//    FindNodeMessage findNodeMessage = new FindNodeMessage(cNode, NetUtil.getNodeId());
//    for (int i = 0; i < 10000; i++) {
//      kad2ServiceTest.send(sever, findNodeMessage);
//    }
//
//    Thread.sleep(10000);

  }
}
