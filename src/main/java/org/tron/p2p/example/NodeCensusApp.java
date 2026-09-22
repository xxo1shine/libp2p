package org.tron.p2p.example;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.P2pService;
import org.tron.p2p.utils.NetUtil;

public final class NodeCensusApp {

  private NodeCensusApp() {
  }

  public static void main(String[] args) throws Exception {
    Options options = Options.parse(args);
    P2pConfig config = new P2pConfig();
    config.getSeedNodes().addAll(options.seeds);
    config.setNetworkId(options.networkId);
    config.setPort(options.p2pPort);
    config.setBucketScanEnable(true);
    config.setMinConnections(0);
    config.setMinActiveConnections(0);
    config.setMaxConnections(0);
    config.setNodeDetectEnable(false);
    if (options.externalIp != null) {
      config.setIp(options.externalIp);
    }

    P2pService service = new P2pService();
    try (NodeCensusServer api = new NodeCensusServer(
        new InetSocketAddress(options.httpHost, options.httpPort),
        service::getPendingNodes)) {
      service.start(config);
      api.start();
      System.out.println("Node census API: http://" + options.httpHost + ":"
          + api.getPort() + "/nodes");
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        api.close();
        service.close();
      }));
      new CountDownLatch(1).await();
    } finally {
      service.close();
    }
  }

  public static final class Options {
    public final List<InetSocketAddress> seeds = new ArrayList<>();
    public String httpHost = "127.0.0.1";
    public int httpPort = 8080;
    public int p2pPort = 18889;
    public int networkId = 11111;
    public String externalIp;

    private Options() {
    }

    public static Options parse(String[] args) {
      Options result = new Options();
      if (args.length % 2 != 0) {
        throw new IllegalArgumentException("Expected --option value pairs");
      }
      for (int i = 0; i < args.length; i += 2) {
        String value = args[i + 1];
        switch (args[i]) {
          case "--seed":
            result.seeds.add(seed(value));
            break;
          case "--http-host":
            result.httpHost = value;
            break;
          case "--http-port":
            result.httpPort = port(value);
            break;
          case "--p2p-port":
            result.p2pPort = port(value);
            break;
          case "--network-id":
            result.networkId = positive(value);
            break;
          case "--external-ip":
            if (!NetUtil.validIpV4(value)) {
              throw new IllegalArgumentException("--external-ip must be an IPv4 address");
            }
            result.externalIp = value;
            break;
          default:
            throw new IllegalArgumentException("Unknown option: " + args[i]);
        }
      }
      if (result.seeds.isEmpty()) {
        throw new IllegalArgumentException("At least one --seed is required");
      }
      return result;
    }

    private static InetSocketAddress seed(String value) {
      int separator = value.lastIndexOf(':');
      if (separator < 1 || separator == value.length() - 1) {
        throw new IllegalArgumentException("Seed must be IP:port");
      }
      String ip = value.substring(0, separator);
      if (ip.startsWith("[") && ip.endsWith("]")) {
        ip = ip.substring(1, ip.length() - 1);
      }
      if (!NetUtil.validIpV4(ip) && !NetUtil.validIpV6(ip)) {
        throw new IllegalArgumentException("Seed must use an IP literal");
      }
      try {
        return new InetSocketAddress(InetAddress.getByName(ip),
            port(value.substring(separator + 1)));
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Invalid seed IP: " + ip, e);
      }
    }

    private static int port(String value) {
      int port = positive(value);
      if (port > 65535) {
        throw new IllegalArgumentException("Port out of range: " + value);
      }
      return port;
    }

    private static int positive(String value) {
      try {
        int number = Integer.parseInt(value);
        if (number > 0) {
          return number;
        }
      } catch (NumberFormatException ignored) {
        // Handled below.
      }
      throw new IllegalArgumentException("Expected positive integer: " + value);
    }
  }
}
