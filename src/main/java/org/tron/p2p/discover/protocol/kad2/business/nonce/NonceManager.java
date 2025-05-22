package org.tron.p2p.discover.protocol.kad2.business.nonce;

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.tron.p2p.utils.NetUtil;

import static org.tron.p2p.base.Constant.CALL_TIMEOUT;
import static org.tron.p2p.base.Constant.SESSION_KEY_SIZE;

@Slf4j(topic = "net")
public class NonceManager {
  private volatile long updateTime;
  private volatile ByteString preNonce;
  private volatile ByteString nonce;
  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder().namingPattern("nonceTimer").build());

  public void init () {
    update();
    timer.scheduleWithFixedDelay(this::update, 5,10, TimeUnit.MINUTES);
    log.debug("NonceManager start.");
  }

  public void close () {
    timer.shutdown();
    log.debug("NonceManager close.");
  }

  public ByteString getNonce(InetSocketAddress address) {
    Keccak.Digest256 digest256 = new  Keccak.Digest256();
    byte[] hostBytes = (address.getHostString() + ":" + address.getPort()).getBytes();
    byte[] bytes = ArrayUtils.addAll(nonce.toByteArray(), hostBytes);
    byte[] hash = digest256.digest(bytes);
    return ByteString.copyFrom(hash);
  }

  public synchronized boolean check(InetSocketAddress address, ByteString n) {
    Keccak.Digest256 digest256 = new  Keccak.Digest256();
    byte[] hostBytes = (address.getHostString() + ":" + address.getPort()).getBytes();
    byte[] bytes = ArrayUtils.addAll(nonce.toByteArray(), hostBytes);
    byte[] hash = digest256.digest(bytes);
    if (Arrays.equals(n.toByteArray(), hash)) {
      return true;
    } else {
      if (updateTime + CALL_TIMEOUT > System.currentTimeMillis() && preNonce != null) {
        bytes = ArrayUtils.addAll(preNonce.toByteArray(), hostBytes);
        hash = digest256.digest(bytes);
        return Arrays.equals(n.toByteArray(), hash);
      }
    }
    return false;
  }

  public synchronized void update() {
    this.preNonce = this.nonce;
    this.nonce = NetUtil.getRandomBytes(SESSION_KEY_SIZE);
    this.updateTime = System.currentTimeMillis();
  }

}
