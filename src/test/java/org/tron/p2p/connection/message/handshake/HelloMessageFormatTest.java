package org.tron.p2p.connection.message.handshake;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.protos.Discover;

public class HelloMessageFormatTest {

  private static final int UNKNOWN_FIELD_NUMBER = 1000;

  private byte[] helloBytesWithUnknown(int unknownSize) {
    byte[] nodeId = new byte[64];
    for (int i = 0; i < nodeId.length; i++) {
      nodeId[i] = (byte) i;
    }
    Discover.Endpoint from = Discover.Endpoint.newBuilder()
        .setNodeId(ByteString.copyFrom(nodeId))
        .setAddress(ByteString.copyFromUtf8("127.0.0.1"))
        .setPort(18888)
        .build();
    Connect.HelloMessage hello = Connect.HelloMessage.newBuilder()
        .setFrom(from).setNetworkId(11).setCode(0).setVersion(2).setTimestamp(123456789L).build();
    UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
        .addField(UNKNOWN_FIELD_NUMBER, UnknownFieldSet.Field.newBuilder()
            .addLengthDelimited(ByteString.copyFrom(new byte[unknownSize]))
            .build())
        .build();
    return hello.toBuilder().setUnknownFields(unknown).build().toByteArray();
  }

  @Test
  public void toStringIsBoundedAndOmitsUnknownFields() throws Exception {
    // Same message, wildly different amounts of attacker padding.
    HelloMessage small = new HelloMessage(helloBytesWithUnknown(16));
    HelloMessage huge = new HelloMessage(helloBytesWithUnknown(1_000_000));

    String smallStr = small.toString();
    String hugeStr = huge.toString();

    // Output size is independent of the unknown-field size: no amplification.
    Assert.assertEquals(smallStr, hugeStr);
    Assert.assertTrue("toString should stay bounded", hugeStr.length() < 1024);
  }

  @Test
  public void toStringPrintsOnlyBoundedScalarFields() throws Exception {
    HelloMessage m = new HelloMessage(helloBytesWithUnknown(32));
    String s = m.toString();

    // Only the three bounded scalar fields are printed.
    Assert.assertEquals("[HelloMessage: networkId: 11, version: 2, code: 0]", s);
    // Byte fields (from/nodeId/address) are never rendered, so nothing can be amplified.
    Assert.assertFalse(s.contains("nodeId"));
    Assert.assertFalse(s.contains("address"));
    Assert.assertFalse(s.contains("127.0.0.1"));
  }
}
