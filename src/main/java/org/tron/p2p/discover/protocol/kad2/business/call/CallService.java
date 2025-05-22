package org.tron.p2p.discover.protocol.kad2.business.call;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad2.RequestMessage;
import org.tron.p2p.discover.message.kad2.ResponseMessage;
import org.tron.p2p.discover.protocol.kad2.business.msg.MsgSender;

import static org.tron.p2p.base.Constant.CALL_TIMEOUT;
import static org.tron.p2p.base.Constant.MAX_NODES;

@Slf4j(topic = "net")
public class CallService {
//  private final static int CALL_TIMEOUT = 5;

  private final Cache<ByteString, Call> calls = CacheBuilder.newBuilder()
          .maximumSize(MAX_NODES).expireAfterWrite(10, TimeUnit.SECONDS).build();

  public Call getCall(ByteString reqId) {
    return calls.getIfPresent(reqId);
  }

  public void handleResponse(ResponseMessage msg, InetSocketAddress address) {
    Call call = calls.getIfPresent(msg.getReqId());
    if (call == null) {
      log.debug("Nodes from {} without reqId.", address);
      return;
    }

    if (!call.getResponseType().equals(msg.getType())) {
      log.debug("Nodes from {} with wrong type.", address);
      return;
    }

    calls.invalidate(msg.getReqId());

    call.getTimer().shutdownNow();

    if (call.getCallBack() != null) {
      call.getCallBack().handleResponse(msg, address);
    }
  }

  public void sendCall(InetSocketAddress address, RequestMessage message, CallBack callBack) {
    MessageType responseType;
    switch (message.getType()) {
      case KAD2_Ping: {
        responseType = MessageType.KAD2_Pong;
        break;
      }
      case KAD2_FindNodes: {
        responseType = MessageType.KAD2_Nodes;
        break;
      }
      default: {
        log.error("Error type {}", message.getType());
        return;
      }
    }
    Call call = new Call(message, responseType, callBack);
    calls.put(message.getReqId(), call);
    MsgSender.sendMessage(message, address);
    call.getTimer().schedule(() -> {
      try {
        if (call.getCallBack() != null) {
          call.getCallBack().timeout(address);
        }
        calls.invalidate(message.getReqId());
      } catch (Exception e) {
        log.error("Unhandled exception in pong timer schedule", e);
      }
    }, CALL_TIMEOUT, TimeUnit.MILLISECONDS);
  }
}
