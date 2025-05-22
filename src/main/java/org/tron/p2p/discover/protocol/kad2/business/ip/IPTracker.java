package org.tron.p2p.discover.protocol.kad2.business.ip;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.utils.NetUtil;

@Slf4j(topic = "net")
public class IPTracker {
  private static final IPTracker ipTrackerV4 = new IPTracker();
  private static final IPTracker ipTrackerV6 = new IPTracker();
  private static final int ipTrackWindow = 5 * 60 * 1000;
  private static final int maxMapSize = 1000;

  private final Map<InetAddress, IPStatement> ipStatementMap = new ConcurrentHashMap<>();
  private volatile long updateTime;

  public static void addStatement(InetAddress ip, InetSocketAddress address) {
    if (NetUtil.isPrivateIp(address.getAddress())) {
      return;
    }

    if (ip instanceof Inet4Address) {
      ipTrackerV4.add(ip, address);
    } else {
      ipTrackerV6.add(ip, address);
    }

    log.debug("KAD2 Add statement {}, {}", ip, address);
  }

  public void add(InetAddress ip, InetSocketAddress address) {
    long now = System.currentTimeMillis();
    boolean hasKey = ipStatementMap.containsKey(ip);
    boolean needCapacity = !hasKey && ipStatementMap.size() >= maxMapSize;
    boolean needPredict = now - updateTime > ipTrackWindow;

    if (needCapacity || needPredict) {
      gcStatements();
    }

    if (hasKey || ipStatementMap.size() < maxMapSize) {
      ipStatementMap.put(ip, new IPStatement(address));
    }

    if (needPredict) {
      setIp(predictEndpoint());
      updateTime = now;
    }
  }

  private void setIp(InetAddress ip) {
    if (ip instanceof Inet4Address) {
      Parameter.p2pConfig.setIp(ip.getHostAddress());
    } else {
      Parameter.p2pConfig.setIpv6(ip.getHostAddress());
    }
    log.debug("KAD2 Set ip {}", ip);
  }

  private void gcStatements() {
    long now = System.currentTimeMillis();
    ipStatementMap.entrySet().removeIf(e ->
            now - e.getValue().getTime() > ipTrackWindow);
  }

  public InetAddress predictEndpoint() {
    Map<InetAddress, Integer> map = new HashMap<>();

    ipStatementMap.values().forEach(v ->
            map.merge(v.getAddress().getAddress(), 1, Integer::sum));

    return map.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey).orElse(null);
  }
}
