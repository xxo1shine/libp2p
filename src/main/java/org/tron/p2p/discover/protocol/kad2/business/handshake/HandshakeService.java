package org.tron.p2p.discover.protocol.kad2.business.handshake;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad2.FindNodes;
import org.tron.p2p.discover.message.kad2.IdentityRequest;
import org.tron.p2p.discover.message.kad2.IdentityResponse;
import org.tron.p2p.discover.message.kad2.Ping;
import org.tron.p2p.discover.message.kad2.Pong;
import org.tron.p2p.discover.protocol.kad2.Kad2Service;
import org.tron.p2p.discover.protocol.kad2.business.call.Call;
import org.tron.p2p.discover.protocol.kad2.business.call.CallBack;
import org.tron.p2p.discover.protocol.kad2.business.call.CallService;
import org.tron.p2p.discover.protocol.kad2.business.ip.IPTracker;
import org.tron.p2p.discover.protocol.kad2.business.msg.MsgSender;
import org.tron.p2p.discover.protocol.kad2.business.session.SessionManager;
import org.tron.p2p.discover.protocol.kad2.business.session.Session;
import org.tron.p2p.discover.protocol.kad2.business.nonce.NonceManager;
import org.tron.p2p.discover.protocol.kad2.table.Table;
import org.tron.p2p.discover.socket.UdpEvent;
import org.tron.p2p.utils.NetUtil;

import static org.tron.p2p.base.Constant.MAX_NODES;
import static org.tron.p2p.base.Constant.REQ_ID_SIZE;
import static org.tron.p2p.base.Constant.SESSION_KEY_SIZE;

@Slf4j(topic = "net")
public class HandshakeService implements CallBack {
  private final NonceManager nonceManager;
  private final Kad2Service kad2Service;
  private final SessionManager sessionManager;
  private final CallService callService;
  private final Table table;
  private final Cache<InetSocketAddress, Long> cache = CacheBuilder.newBuilder()
          .maximumSize(MAX_NODES).expireAfterWrite(10, TimeUnit.MINUTES).build();

  public HandshakeService(NonceManager nonceManager,
                          SessionManager sessionManager,
                          CallService callService,
                          Table table,
                          Kad2Service kad2Service) {
    this.nonceManager = nonceManager;
    this.kad2Service = kad2Service;
    this.sessionManager = sessionManager;
    this.callService = callService;
    this.table = table;
  }

  public void handleUnknown(InetSocketAddress address, ByteString reqId) {
    ByteString nonce = nonceManager.getNonce(address);
    log.debug("KAD2 handleUnknown {}, {}", reqId, nonce);
    IdentityRequest request = new IdentityRequest(reqId, nonce);
    MsgSender.sendMessage(request, address);
  }

  public void handleIdentityRequest(IdentityRequest msg, InetSocketAddress address) {
    Call call = callService.getCall(msg.getReqId());
    if (call == null) {
      log.debug("KAD2 Receive IdentityRequest from {} without request.", address);
      return;
    }

    if (call.isRequested()) {
      log.debug("KAD2 Receive IdentityRequest from {} with two requests.", address);
      return;
    }

    call.setRequested(true);

    ByteString key;
    Session session = sessionManager.getSession(address);
    if (session != null) {
      key = session.getKey();
    } else {
      key = NetUtil.getRandomBytes(SESSION_KEY_SIZE);
    }

    Node node = new Node(address, msg.getNodeId().toByteArray(), msg.getPort());
    sessionManager.addNode(node, key);
    table.addNode(node);

    Message requestMsg = getMsg(key, call);
    IdentityResponse response = new IdentityResponse(msg.getNonce(), key, requestMsg);
    MsgSender.sendMessage(response, address);
  }

  public void handleIdentityResponse(IdentityResponse msg, InetSocketAddress address) {
    if (!nonceManager.check(address, msg.getNonce())) {
      log.debug("KAD2 Check nonce failed {}.", address);
      return;
    }

    Node node = new Node(address, msg.getNodeId().toByteArray(), msg.getPort());
    ByteString key = msg.getSession();
    sessionManager.addNode(node, key);
    table.addNode(node);

    Message m = msg.getMsg();
    if (m != null) {
      kad2Service.handleEvent(new UdpEvent(m, address));
    }
  }

  public void initHandshake(InetSocketAddress address) {
    if (address.getAddress() instanceof Inet4Address) {
      if (address.getAddress().getHostAddress().equals(Parameter.p2pConfig.getIp())) {
        return;
      }
    } else {
      if (address.getAddress().getHostAddress().equals(Parameter.p2pConfig.getIpv6())) {
        return;
      }
    }

    if (cache.getIfPresent(address) != null
      || sessionManager.getSession(address) != null) {
      return;
    }
    cache.put(address, System.currentTimeMillis());
    Ping ping = new Ping(NetUtil.getRandomBytes(REQ_ID_SIZE), null);
    callService.sendCall(address, ping, this);
  }

  private Message getMsg(ByteString sessionKey, Call call) {
    Message msg = null;
    MessageType type = call.getResponseType();
    switch (call.getResponseType()) {
      case KAD2_Pong: {
        Ping ping = (Ping) call.getMessage();
        msg = new Ping(ping.getReqId(), sessionKey);
        break;
      }
      case KAD2_Nodes: {
        FindNodes findNodes = (FindNodes) call.getMessage();
        msg = new FindNodes(findNodes.getReqId(), sessionKey, findNodes.getTargetId());
        break;
      }
      default: {
        log.error("KAD2 Error response type {}.", type);
        break;
      }
    }
    return msg;
  }

  @Override
  public void handleResponse(Message message, InetSocketAddress address) {
    log.debug("KAD2 HandshakeService process response from {}", address);
    Pong pong = (Pong) message;
    IPTracker.addStatement(address.getAddress(), pong.getToIp());
  }

  @Override
  public void timeout(InetSocketAddress address) {
    log.debug("KAD2 HandshakeService process timeout from {}", address);
  }
}
