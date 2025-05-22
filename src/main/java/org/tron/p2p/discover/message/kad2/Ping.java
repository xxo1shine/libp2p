package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import java.util.List;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.protos.Discover;

public class Ping  extends RequestMessage {
  private final Discover.Ping ping;

  public Ping(byte[] data) throws Exception {
    super(MessageType.KAD2_Ping, data);
    this.ping = Discover.Ping.parseFrom(data);
  }

  public Ping(ByteString reqId, ByteString session) {
    super(MessageType.KAD2_Ping, null);
    Discover.Ping.Builder builder = Discover.Ping.newBuilder().setReqId(reqId);
    if (session != null) {
      builder.setSession(session);
    }
    this.ping = builder.build();
    this.data = this.ping.toByteArray();
  }

  public ByteString getSession() {
    return this.ping.getSession();
  }

  public ByteString getReqId() {
    return this.ping.getReqId();
  }

  public List<Node> getNodes() {
    return null;
  }

  @Override
  public String toString() {
    return "[Ping: " + ping;
  }

  @Override
  public boolean valid() {
    return true;
  }

}
