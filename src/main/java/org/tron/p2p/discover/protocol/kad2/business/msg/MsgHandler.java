package org.tron.p2p.discover.protocol.kad2.business.msg;

import java.net.InetSocketAddress;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.kad2.FindNodes;
import org.tron.p2p.discover.message.kad2.IdentityRequest;
import org.tron.p2p.discover.message.kad2.IdentityResponse;
import org.tron.p2p.discover.message.kad2.Nodes;
import org.tron.p2p.discover.message.kad2.Ping;
import org.tron.p2p.discover.message.kad2.Pong;
import org.tron.p2p.discover.message.kad2.RequestMessage;
import org.tron.p2p.discover.message.kad2.ResponseMessage;
import org.tron.p2p.discover.protocol.kad2.business.call.CallService;
import org.tron.p2p.discover.protocol.kad2.business.handshake.HandshakeService;
import org.tron.p2p.discover.protocol.kad2.business.session.SessionManager;
import org.tron.p2p.discover.protocol.kad2.business.session.Session;
import org.tron.p2p.discover.protocol.kad2.table.Table;

@Slf4j(topic = "net")
public class MsgHandler {
  private final SessionManager sessionManager;
  private final CallService callService;
  private final HandshakeService handshakeService;
  private final Table table;

  public MsgHandler(SessionManager nodeManager,
                    CallService callService,
                    HandshakeService handshakeService,
                    Table table) {
    this.sessionManager = nodeManager;
    this.callService = callService;
    this.handshakeService = handshakeService;
    this.table = table;
  }

  public void handleMsg(Message msg, InetSocketAddress address) {
    switch (msg.getType()) {
      case KAD2_IdentityRequest: {
        handshakeService.handleIdentityRequest((IdentityRequest) msg, address);
        break;
      }
      case KAD2_IdentityResponse: {
        handshakeService.handleIdentityResponse((IdentityResponse) msg, address);
        break;
      }
      case KAD2_Ping: {
        handlePing((Ping) msg, address);
        break;
      }
      case KAD2_FindNodes: {
        handleFindNodes((FindNodes) msg, address);
        break;
      }
      case KAD2_Pong:
      case KAD2_Nodes: {
        callService.handleResponse((ResponseMessage) msg, address);
        break;
      }
      default: {
        log.debug("KAD2 Receive unknown message {} from {}", msg.getType(), address);
        break;
      }
    }
  }

  private boolean check(RequestMessage msg, InetSocketAddress address) {
    Session session = sessionManager.getSession(address);
    if (session == null || !session.getKey().equals(msg.getSession())) {
      log.debug("KAD2 Receive RequestMessage from {} without session {},{}.", address, msg.getSession(), session);
      handshakeService.handleUnknown(address, msg.getReqId());
      return false;
    }
    return session.tryAcquire();
  }

  public void handlePing(Ping msg, InetSocketAddress address) {
    if (check(msg, address)) {
      MsgSender.sendMessage(new Pong(msg.getReqId(), address), address);
    }
  }

  public void handleFindNodes(FindNodes msg, InetSocketAddress address) {
    if (check(msg, address)) {
      List<Node> closest = table.getClosestNodes(msg.getTargetId().toByteArray(), address.getAddress());
      Nodes nodes = new Nodes(msg.getReqId(), closest);
      MsgSender.sendMessage(nodes, address);
    }
  }

}
