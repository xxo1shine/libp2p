package org.tron.p2p.discover.protocol.kad2.business;

import org.junit.Assert;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.protocol.kad2.business.ip.IPStatement;
import org.tron.p2p.discover.protocol.kad2.business.ip.IPTracker;
import org.tron.p2p.utils.NetUtil;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

public class IPTrackerTest {

  @Test
  public void testAddStatement() {
    Parameter.p2pConfig = new P2pConfig();

    InetSocketAddress a1 = new InetSocketAddress("1.1.1.1", 1000);
    InetSocketAddress a2 = new InetSocketAddress("192.168.0.1", 1000);
    InetSocketAddress a3 = new InetSocketAddress("1.1.1.2", 1000);

    IPTracker.addStatement(a1.getAddress(), a2);

    System.out.println(NetUtil.isPrivateIp(a2.getAddress()));

    Assert.assertTrue(!a2.getHostString().equals(Parameter.p2pConfig.getIp()));

    IPTracker.addStatement(a1.getAddress(), a3);

    Assert.assertEquals(Parameter.p2pConfig.getIp(), a3.getHostString());
  }

  @Test
  public void testAdd() throws Exception {
    Parameter.p2pConfig = new P2pConfig();
    IPTracker ipTracker = new IPTracker();

    InetSocketAddress a1 = new InetSocketAddress("1.1.1.1", 1000);
    InetSocketAddress a2 = new InetSocketAddress("fe80::1873:f3df:65cb:5ba9", 1000);

    ipTracker.add(a1.getAddress(), a2);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a2.getHostString());

    InetSocketAddress a3 = new InetSocketAddress("fe80::1873:f3df:65cb:5ba8", 1000);

    ipTracker.add(new InetSocketAddress("1.1.1.2", 1000).getAddress(), a3);
    ipTracker.add(new InetSocketAddress("1.1.1.3", 1000).getAddress(), a3);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a2.getHostString());

    Field declaredField = ipTracker.getClass().getDeclaredField("updateTime");
    declaredField.setAccessible(true);
    declaredField.set(ipTracker, 0);

    ipTracker.add(new InetSocketAddress("1.1.1.3", 1000).getAddress(), a3);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a3.getHostString());

    Field d = ipTracker.getClass().getDeclaredField("ipStatementMap");
    d.setAccessible(true);
    Map<InetAddress, IPStatement> map = (Map<InetAddress, IPStatement>)d.get(ipTracker);

    Assert.assertEquals(map.size(), 3);

    InetSocketAddress a4 = new InetSocketAddress("fe80::1873:f3df:65cb:5ba7", 1000);
    for (int i = 0; i < 1000 - 3; i++) {
      String ip = "1.22." + (i / 10) + "." + (i % 256);
      InetAddress address = new InetSocketAddress(ip, 1000).getAddress();
      ipTracker.add(address, a4);
    }

    Assert.assertEquals(map.size(), 1000);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a3.getHostString());

    InetSocketAddress a5 = new InetSocketAddress("3.3.3.3", 1000);
    ipTracker.add(a5.getAddress(), a4);

    Assert.assertEquals(map.size(), 1000);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a3.getHostString());

    Assert.assertEquals(map.get(a5.getAddress()), null);

    IPStatement ipStatement = map.get(a1.getAddress());
    ipStatement.setTime(0);
    ipStatement = map.get(new InetSocketAddress("1.1.1.3", 1000).getAddress());
    ipStatement.setTime(0);

    ipTracker.add(a5.getAddress(), a4);
    Assert.assertEquals(map.size(), 999);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a3.getHostString());

    declaredField.set(ipTracker, 0);
    ipTracker.add(a5.getAddress(), a4);
    Assert.assertEquals(map.size(), 999);
    Assert.assertEquals(Parameter.p2pConfig.getIpv6(), a4.getHostString());
  }

}
