package org.tron.p2p.discover.protocol.kad2.business;

import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.discover.protocol.kad2.business.nonce.NonceManager;

import java.net.InetSocketAddress;

public class NonceManagerTest {

  @Test
  public void testGetNonce() {
    NonceManager nonceManager = new NonceManager();
    nonceManager.update();

    InetSocketAddress a1 = new InetSocketAddress("1.1.1.1", 1000);
    InetSocketAddress a2 = new InetSocketAddress("1.1.1.1", 2000);

    ByteString n1 = nonceManager.getNonce(a1);
    ByteString n2 = nonceManager.getNonce(a1);
    ByteString n3 = nonceManager.getNonce(a2);

    Assert.assertTrue(n1.equals(n2));
    Assert.assertFalse(n1.equals(n3));
  }

  @Test
  public void testCheck() {
    NonceManager nonceManager = new NonceManager();
    nonceManager.update();

    InetSocketAddress a1 = new InetSocketAddress("1.1.1.1", 1000);
    InetSocketAddress a2 = new InetSocketAddress("1.1.1.2", 1000);

    ByteString n1 = nonceManager.getNonce(a1);

    boolean b1 = nonceManager.check(a1, n1);
    boolean b2 = nonceManager.check(a2, n1);

    Assert.assertTrue(b1);
    Assert.assertFalse(b2);
  }
}
