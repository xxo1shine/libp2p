package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import org.tron.p2p.discover.message.Message;
import org.tron.p2p.discover.message.MessageType;

public abstract class RequestMessage extends Message {

  protected RequestMessage(MessageType type, byte[] data) {
    super(type, data);
  }

  public abstract ByteString getReqId();

  public abstract ByteString getSession();
}
