package org.tron.p2p.discover.protocol.kad2.business;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad2.Nodes;
import org.tron.p2p.discover.message.kad2.Ping;
import org.tron.p2p.discover.message.kad2.Pong;
import org.tron.p2p.discover.protocol.kad2.business.call.Call;
import org.tron.p2p.discover.protocol.kad2.business.call.CallService;
import org.tron.p2p.utils.NetUtil;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.tron.p2p.base.Constant.MAX_NODES;

public class CallServiceTest {

  @Test
  public void testHandleResponse() throws Exception {
    CallService service = new CallService();

    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 1000);

    Cache<ByteString, Call> calls = CacheBuilder.newBuilder()
      .maximumSize(MAX_NODES).expireAfterWrite(10, TimeUnit.SECONDS).build();

    Field declaredField = service.getClass().getDeclaredField("calls");
    declaredField.setAccessible(true);
    declaredField.set(service, calls);

    ByteString reqId = NetUtil.getRandomBytes(8);
    Ping ping = new Ping(reqId, NetUtil.getRandomBytes(8));

    Call call = new Call(ping, MessageType.KAD2_Pong, null);
    calls.put(reqId, call);

    Pong pong = new Pong(NetUtil.getRandomBytes(7), address);
    service.handleResponse(pong, address);
    Assert.assertTrue(calls.getIfPresent(reqId) != null);

    Nodes nodes = new Nodes(reqId, new ArrayList<>());
    service.handleResponse(pong, address);
    Assert.assertTrue(calls.getIfPresent(reqId) != null);

    pong = new Pong(reqId, address);
    service.handleResponse(pong, address);
    Assert.assertTrue(calls.getIfPresent(reqId) == null);
  }

  @Test
  public void testSendCall() throws Exception  {
    CallService service = new CallService();

    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 1000);

    Cache<ByteString, Call> calls = CacheBuilder.newBuilder()
      .maximumSize(MAX_NODES).expireAfterWrite(10, TimeUnit.SECONDS).build();

    Field declaredField = service.getClass().getDeclaredField("calls");
    declaredField.setAccessible(true);
    declaredField.set(service, calls);

    ByteString reqId = NetUtil.getRandomBytes(8);
    Ping ping = new Ping(reqId, NetUtil.getRandomBytes(8));
    service.sendCall(address, ping, null);

    Assert.assertTrue(calls.getIfPresent(reqId) != null);

    Call call = calls.getIfPresent(reqId);
    Assert.assertFalse(call.getTimer().isShutdown());

    Pong pong = new Pong(reqId, address);
    service.handleResponse(pong, address);
    Assert.assertTrue(calls.getIfPresent(reqId) == null);
    Assert.assertTrue(call.getTimer().isShutdown());
  }

}
