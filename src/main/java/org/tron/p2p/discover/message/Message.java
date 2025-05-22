package org.tron.p2p.discover.message;

import org.apache.commons.lang3.ArrayUtils;
import org.tron.p2p.discover.message.kad.FindNodeMessage;
import org.tron.p2p.discover.message.kad.NeighborsMessage;
import org.tron.p2p.discover.message.kad.PingMessage;
import org.tron.p2p.discover.message.kad.PongMessage;
import org.tron.p2p.discover.message.kad2.FindNodes;
import org.tron.p2p.discover.message.kad2.IdentityRequest;
import org.tron.p2p.discover.message.kad2.IdentityResponse;
import org.tron.p2p.discover.message.kad2.Nodes;
import org.tron.p2p.discover.message.kad2.Ping;
import org.tron.p2p.discover.message.kad2.Pong;
import org.tron.p2p.exception.P2pException;

public abstract class Message {
  protected MessageType type;
  protected byte[] data;

  protected Message(MessageType type, byte[] data) {
    this.type = type;
    this.data = data;
  }

  public static Message parse(byte[] encode) throws Exception {
    byte type = encode[0];
    byte[] data = ArrayUtils.subarray(encode, 1, encode.length);
    Message message;
    switch (MessageType.fromByte(type)) {
      case KAD_PING:
        message = new PingMessage(data);
        break;
      case KAD_PONG:
        message = new PongMessage(data);
        break;
      case KAD_FIND_NODE:
        message = new FindNodeMessage(data);
        break;
      case KAD_NEIGHBORS:
        message = new NeighborsMessage(data);
        break;
      case KAD2_Ping:
        message = new Ping(data);
        break;
      case KAD2_Pong:
        message = new Pong(data);
        break;
      case KAD2_IdentityRequest:
        message = new IdentityRequest(data);
        break;
      case KAD2_IdentityResponse:
        message = new IdentityResponse(data);
        break;
      case KAD2_FindNodes:
        message = new FindNodes(data);
        break;
      case KAD2_Nodes:
        message = new Nodes(data);
        break;
      default:
        throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, "type=" + type);
    }
    if (!message.valid()) {
      throw new P2pException(P2pException.TypeEnum.BAD_MESSAGE, "type=" + type);
    }
    return message;
  }

  public MessageType getType() {
    return this.type;
  }

  public byte[] getData() {
    return this.data;
  }

  public byte[] getSendData() {
    return ArrayUtils.add(this.data, 0, type.getType());
  }

  public abstract boolean valid();

  @Override
  public String toString() {
    return "[Message Type: " + getType() + ", len: " + (data == null ? 0 : data.length) + "]";
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

}
