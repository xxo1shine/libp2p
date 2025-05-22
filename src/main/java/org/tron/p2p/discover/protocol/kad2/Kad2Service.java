package org.tron.p2p.discover.protocol.kad2;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.DiscoverService;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.protocol.kad2.business.call.CallService;
import org.tron.p2p.discover.protocol.kad2.business.handshake.HandshakeService;
import org.tron.p2p.discover.protocol.kad2.business.ip.IPTracker;
import org.tron.p2p.discover.protocol.kad2.business.keepalive.KeepAliveService;
import org.tron.p2p.discover.protocol.kad2.business.lookup.LookupTask;
import org.tron.p2p.discover.protocol.kad2.business.msg.MsgHandler;
import org.tron.p2p.discover.protocol.kad2.business.msg.MsgSender;
import org.tron.p2p.discover.protocol.kad2.business.session.SessionManager;
import org.tron.p2p.discover.protocol.kad2.business.nonce.NonceManager;
import org.tron.p2p.discover.protocol.kad2.table.Table;
import org.tron.p2p.discover.socket.UdpEvent;

@Slf4j(topic = "net")
public class Kad2Service implements DiscoverService {
  private final Set<InetSocketAddress> bootNodes = new HashSet<>();
  private volatile boolean activated = false;
  private final NonceManager nonceManager;
  private final SessionManager sessionManager;
  private final Table table;
  private final MsgHandler msgHandler;
  private final LookupTask discoverTask;
  private final HandshakeService handshakeService;
  private final KeepAliveService keepAliveService;

  public Kad2Service() {
    CallService callService = new CallService();
    this.nonceManager = new NonceManager();
    this.sessionManager = new SessionManager();
    this.table = new Table();
    this.keepAliveService = new KeepAliveService(callService, sessionManager, table);
    this.handshakeService = new HandshakeService(nonceManager, sessionManager, callService, table, this);
    this.discoverTask = new LookupTask(sessionManager, callService, handshakeService, keepAliveService, table);
    this.msgHandler = new MsgHandler(sessionManager, callService, handshakeService, table);
  }

  @Override
  public void init() {

    if (Parameter.p2pConfig.getSeedNodes() != null) {
      bootNodes.addAll(Parameter.p2pConfig.getSeedNodes());
    }
    if (Parameter.p2pConfig.getActiveNodes() != null) {
      bootNodes.addAll(Parameter.p2pConfig.getActiveNodes());
    }

    table.register(keepAliveService);
    table.register(sessionManager);

    if (Parameter.p2pConfig.isDiscoverEnable() && Parameter.p2pConfig.isKad2Enable()) {
      nonceManager.init();
      discoverTask.init();
      keepAliveService.init();
    }
    log.debug("Kad2 service start.");
  }

  @Override
  public void close() {
    try {
      nonceManager.close();
      discoverTask.close();
      keepAliveService.close();
    } catch (Exception e) {
      log.error("Close Kad2 service failed.", e);
    }
    log.debug("Kad2 service close.");
  }

  @Override
  public void setMessageSender(Consumer<UdpEvent> messageSender) {
    MsgSender.setMessageSender(messageSender);
  }

  @Override
  public Node getPublicHomeNode() {
    return new Node(Parameter.p2pConfig.getNodeID(),
            Parameter.p2pConfig.getIp(),
            Parameter.p2pConfig.getIpv6(),
            Parameter.p2pConfig.getPort());
  }

  @Override
  public List<Node> getTableNodes() {
    return table.getTableNodes();
  }

  @Override
  public List<Node> getAllNodes() {
    return sessionManager.getAllNodes();
  }

  @Override
  public List<Node> getConnectableNodes() {
    return sessionManager.getConnectableNodes();
  }

  @Override
  public void channelActivated() {
    if (!activated) {
      activated = true;
      for (InetSocketAddress address : bootNodes) {
        handshakeService.initHandshake(address);
      }
    }
  }

  @Override
  public void handleEvent(UdpEvent udpEvent) {
    Message msg = udpEvent.getMessage();
    InetSocketAddress address = udpEvent.getAddress();
    msgHandler.handleMsg(msg, address);
  }
}
