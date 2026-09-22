package org.tron.p2p.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.tron.p2p.discover.protocol.kad.NeighborsHandler.PendingNode;

public final class NodeCensusServer implements AutoCloseable {

  private final HttpServer server;
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  private final Supplier<List<PendingNode>> nodes;

  public NodeCensusServer(InetSocketAddress bind, Supplier<List<PendingNode>> nodes)
      throws IOException {
    this.server = HttpServer.create(bind, 32);
    this.nodes = nodes;
    server.setExecutor(executor);
    server.createContext("/nodes", this::handle);
  }

  public void start() {
    server.start();
  }

  public int getPort() {
    return server.getAddress().getPort();
  }

  private void handle(HttpExchange exchange) throws IOException {
    if (!"/nodes".equals(exchange.getRequestURI().getPath())) {
      respond(exchange, 404, "{\"error\":\"not found\"}");
      return;
    }
    if (!"GET".equals(exchange.getRequestMethod())) {
      respond(exchange, 405, "{\"error\":\"method not allowed\"}");
      return;
    }
    StringBuilder json = new StringBuilder("[");
    boolean first = true;
    for (PendingNode node : nodes.get()) {
      if (!first) {
        json.append(',');
      }
      first = false;
      json.append("{\"ip\":\"").append(node.getIp())
          .append("\",\"port\":").append(node.getPort())
          .append(",\"bindPort\":").append(node.getBindPort())
          .append(",\"k\":").append(node.getK())
          .append(",\"cycle\":").append(node.getCycle())
          .append(",\"findNodeFailures\":").append(node.getFindNodeFailures())
          .append('}');
    }
    respond(exchange, 200, json.append(']').toString());
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    exchange.getResponseHeaders().set("Cache-Control", "no-store");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
