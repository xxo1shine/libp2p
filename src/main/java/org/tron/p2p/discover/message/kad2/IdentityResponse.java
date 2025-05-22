package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.protos.Discover;

public class IdentityResponse  extends Message {
  private final Discover.Identity identityResponse;

  public IdentityResponse(byte[] data) throws Exception {
    super(MessageType.KAD2_IdentityResponse, data);
    this.identityResponse = Discover.Identity.parseFrom(data);
  }

  public IdentityResponse(ByteString nonce, ByteString session, Message message) {
    super(MessageType.KAD2_IdentityResponse, null);
    Discover.Node node = Discover.Node.newBuilder()
            .setPort(Parameter.getHomeNode().getPort())
            .setNodeId(Parameter.getHomeNode().getNodeId()).build();
    Discover.Identity.Builder builder = Discover.Identity.newBuilder()
            .setNode(node)
            .setNonce(nonce)
            .putParams(NodeMeta.session, session);
    if (message != null) {
      builder.putParams(NodeMeta.data, ByteString.copyFrom(message.getSendData()));
    }
    this.identityResponse = builder.build();
    this.data = this.identityResponse.toByteArray();
  }

  public ByteString getNonce() {
    return this.identityResponse.getNonce();
  }

  public int getPort() {
    return this.identityResponse.getNode().getPort();
  }

  public ByteString getNodeId() {
    return this.identityResponse.getNode().getNodeId();
  }

  public ByteString getSession() {
    return ByteString.copyFrom(this.identityResponse.getParamsMap().get(NodeMeta.session).toByteArray());
  }

  public Message getMsg() {
    try {
      return Message.parse(this.identityResponse.getParamsMap().get(NodeMeta.data).toByteArray());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String toString() {
    return "[IdentityResponse: " + identityResponse;
  }

  @Override
  public boolean valid() {
    return true;
  }
}
