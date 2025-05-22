package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.protos.Discover;

public class IdentityRequest extends Message {

  private final Discover.Identity identityRequest;

  public IdentityRequest(byte[] data) throws Exception {
    super(MessageType.KAD2_IdentityRequest, data);
    this.identityRequest = Discover.Identity.parseFrom(data);
  }

  public IdentityRequest(ByteString reqId, ByteString nonce) {
    super(MessageType.KAD2_IdentityRequest, null);

    Discover.Node node = Discover.Node.newBuilder()
            .setPort(Parameter.p2pConfig.getPort())
            .setNodeId(ByteString.copyFrom(Parameter.p2pConfig.getNodeID())).build();

    this.identityRequest = Discover.Identity.newBuilder()
            .setNode(node)
            .setNonce(nonce)
            .putParams(NodeMeta.reqId, reqId).build();

    this.data = this.identityRequest.toByteArray();
  }

  public ByteString getNonce() {
    return this.identityRequest.getNonce();
  }

  public ByteString getReqId() {
    return ByteString.copyFrom(this.identityRequest.getParamsMap().get(NodeMeta.reqId).toByteArray());
  }

  public int getPort() {
    return this.identityRequest.getNode().getPort();
  }

  public ByteString getNodeId() {
    return this.identityRequest.getNode().getNodeId();
  }

  @Override
  public String toString() {
    return "[IdentityRequest: " + identityRequest;
  }

  @Override
  public boolean valid() {
    return true;
  }
}
