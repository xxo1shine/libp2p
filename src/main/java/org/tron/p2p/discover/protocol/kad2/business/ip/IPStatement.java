package org.tron.p2p.discover.protocol.kad2.business.ip;

import java.net.InetSocketAddress;
import lombok.Data;

@Data
public class IPStatement {
  private InetSocketAddress address;
  private long time;

  public IPStatement(InetSocketAddress address) {
    this.address = address;
    this.time = System.currentTimeMillis();
  }
}
