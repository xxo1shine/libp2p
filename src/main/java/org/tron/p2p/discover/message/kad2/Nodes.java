package org.tron.p2p.discover.message.kad2;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.protos.Discover;
import org.tron.p2p.utils.ByteArray;

public class Nodes extends ResponseMessage {

  private final Discover.Nodes nodes;

  public Nodes(byte[] data) throws Exception {
    super(MessageType.KAD2_Nodes, data);
    this.nodes = Discover.Nodes.parseFrom(data);
  }

  public Nodes(ByteString reqId, List<Node> list) {
    super(MessageType.KAD2_Nodes, null);
    Discover.Nodes.Builder builder = Discover.Nodes.newBuilder().setReqId(reqId);
    for (Node node: list) {
      String ip = node.getInetSocketAddress().getAddress().getHostAddress();
      int port = node.getInetSocketAddress().getPort();
      Discover.Node n = Discover.Node.newBuilder()
              .setIp(ByteString.copyFrom(ByteArray.fromString(ip)))
              .setPort(port).build();
      builder.addNodes(n);
    }
    this.nodes = builder.build();
    this.data = this.nodes.toByteArray();
  }

  public ByteString getReqId() {
    return this.nodes.getReqId();
  }

  public List<Node> getNodes() {
    return this.nodes.getNodesList().stream()
            .map(n -> new Node(new InetSocketAddress(new String(n.getIp().toByteArray()), n.getPort())))
            .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "[Nodes: " + nodes;
  }

  @Override
  public boolean valid() {
    return true;
  }
}
