package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.protos.Discover;

public class FindNodes extends RequestMessage {

  private final Discover.FindNodes findNodes;

  public FindNodes(byte[] data) throws Exception {
    super(MessageType.KAD2_FindNodes, data);
    this.findNodes = Discover.FindNodes.parseFrom(data);
  }

  public FindNodes(ByteString reqId, ByteString session, ByteString targetId) {
    super(MessageType.KAD2_FindNodes, null);
    this.findNodes = Discover.FindNodes.newBuilder()
            .setReqId(reqId)
            .setSession(session)
            .setTargetId(targetId)
            .build();
    this.data = this.findNodes.toByteArray();
  }

  public ByteString getTargetId() {
    return this.findNodes.getTargetId();
  }

  public ByteString getReqId() {
    return this.findNodes.getReqId();
  }

  public ByteString getSession() {
    return this.findNodes.getSession();
  }

  @Override
  public String toString() {
    return "[FindNodes: " + findNodes;
  }

  @Override
  public boolean valid() {
    return true;
  }
}
