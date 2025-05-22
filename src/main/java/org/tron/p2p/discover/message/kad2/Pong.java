package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.protos.Discover;

public class Pong  extends ResponseMessage {
  private final Discover.Pong pong;

  public Pong(byte[] data) throws Exception {
    super(MessageType.KAD2_Pong, data);
    this.pong = Discover.Pong.parseFrom(data);
  }

  public Pong(ByteString reqId, InetSocketAddress address) {
    super(MessageType.KAD2_Pong, null);
    String ip = address.getAddress().getHostAddress();
    int port = address.getPort();
    String toIp = ip + "/" + port;
    this.pong = Discover.Pong.newBuilder()
            .setReqId(reqId)
            .setToIp(ByteString.copyFrom(toIp.getBytes())).build();
    this.data = this.pong.toByteArray();
  }

  public ByteString getReqId() {
    return this.pong.getReqId();
  }

  public InetSocketAddress getToIp() {
    String[] sz = this.pong.getToIp().toStringUtf8().split("/");
    return new InetSocketAddress(sz[0], Integer.valueOf(sz[1]));
  }

  @Override
  public String toString() {
    return "[Pong: " + pong;
  }

  @Override
  public boolean valid() {
    return true;
  }

}
