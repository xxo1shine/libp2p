package org.tron.p2p.discover.protocol.kad2.business.session;

import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.Setter;
import org.tron.p2p.discover.Node;

public class Session {
  private final RateLimiter rateLimiter;
  @Getter
  private final Node node;
  @Getter
  private final ByteString key;
  @Getter
  private final long handshakeTime;
  @Getter
  @Setter
  private boolean isTableNode;

  public Session(Node node, ByteString key) {
    this.node = node;
    this.key = key;
    this.handshakeTime = System.currentTimeMillis();
    this.rateLimiter = RateLimiter.create(Double.POSITIVE_INFINITY);
    this.rateLimiter.setRate(10);
  }

  public boolean tryAcquire() {
    return rateLimiter.tryAcquire();
  }
}
