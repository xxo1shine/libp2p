package org.tron.p2p.discover;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.protocol.kad.KadService;
import org.tron.p2p.discover.protocol.kad2.Kad2Service;
import org.tron.p2p.discover.socket.DiscoverServer;

@Slf4j(topic = "net")
public class NodeManager {

  private static DiscoverService discoverService;
  private static DiscoverServer discoverServer;
  private static Kad2Service kad2Service;
  public static void init() {
    kad2Service = new Kad2Service();
    discoverService = new KadService(kad2Service);

    discoverService.init();
    if (Parameter.p2pConfig.isDiscoverEnable()) {
      discoverServer = new DiscoverServer();
      discoverServer.init(discoverService);
    }
  }

  public static void close() {
    if (discoverService != null) {
      discoverService.close();
    }
    if (discoverServer != null) {
      discoverServer.close();
    }
    if (kad2Service != null) {
      kad2Service.close();
    }
  }

  public static List<Node> getConnectableNodes() {
    return merge(discoverService.getConnectableNodes(), kad2Service.getTableNodes());
  }

  public static Node getHomeNode() {
    return discoverService.getPublicHomeNode();
  }

  public static List<Node> getTableNodes() {
    return merge(discoverService.getTableNodes(), kad2Service.getTableNodes());
  }

  public static List<Node> getAllNodes() {
    return merge(discoverService.getAllNodes(), kad2Service.getAllNodes());
  }

  private static List<Node> merge(List<Node> src, List<Node> des) {

    Map<InetSocketAddress, Node> map = new HashMap<>();
    src.forEach(v -> map.put(v.getInetSocketAddress(), v));
    des.forEach(v -> map.put(v.getInetSocketAddress(), v));
    log.debug("Get nodes size {}/{}, total {}", src.size(), des.size(), map.size());
    return new ArrayList<>(map.values());
  }

}
