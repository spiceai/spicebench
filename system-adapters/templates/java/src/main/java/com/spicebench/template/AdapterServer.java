package com.spicebench.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public final class AdapterServer {
  private static final String JSONRPC_VERSION = "2.0";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AdapterServer() {}

  public static void main(String[] args) throws Exception {
    String transport = "stdio";
    String host = "127.0.0.1";
    int port = 8080;
    String path = "/jsonrpc";

    for (int i = 0; i < args.length; i++) {
      String token = args[i];
      if ("--transport".equals(token) && i + 1 < args.length) {
        transport = args[++i];
      } else if ("--host".equals(token) && i + 1 < args.length) {
        host = args[++i];
      } else if ("--port".equals(token) && i + 1 < args.length) {
        port = Integer.parseInt(args[++i]);
      } else if ("--path".equals(token) && i + 1 < args.length) {
        path = args[++i];
      }
    }

    if ("stdio".equals(transport)) {
      runStdio();
      return;
    }

    if ("http".equals(transport)) {
      runHttp(host, port, path);
      return;
    }

    System.err.println("Invalid --transport value. Use stdio or http.");
    System.exit(1);
  }

  private static void runStdio() throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      ObjectNode response = processPayload(trimmed.getBytes(StandardCharsets.UTF_8));
      System.out.println(MAPPER.writeValueAsString(response));
      System.out.flush();
    }
  }

  private static void runHttp(String host, int port, String path) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
    server.createContext(
        path,
        exchange -> {
          if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
          }

          byte[] requestBody = exchange.getRequestBody().readAllBytes();
          ObjectNode response = processPayload(requestBody);
          byte[] payload = MAPPER.writeValueAsBytes(response);

          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, payload.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
          }
        });
    server.start();
    System.err.printf("JSON-RPC HTTP server listening on http://%s:%d%s%n", host, port, path);
  }

  private static ObjectNode processPayload(byte[] payload) {
    try {
      JsonNode request = MAPPER.readTree(payload);
      return dispatch(request);
    } catch (Exception ex) {
      return jsonrpcError(null, -32700, "Parse error", ex.getMessage());
    }
  }

  private static ObjectNode dispatch(JsonNode request) {
    JsonNode id = request.has("id") ? request.get("id") : null;

    if (!JSONRPC_VERSION.equals(request.path("jsonrpc").asText(""))) {
      return jsonrpcError(id, -32600, "Invalid Request: jsonrpc must be '2.0'", null);
    }

    JsonNode methodNode = request.get("method");
    if (methodNode == null || !methodNode.isTextual()) {
      return jsonrpcError(id, -32600, "Invalid Request: method must be a string", null);
    }

    String method = methodNode.asText();
    JsonNode params = request.has("params") ? request.get("params") : MAPPER.createObjectNode();
    if (!params.isObject()) {
      return jsonrpcError(id, -32602, "Invalid params: expected object", null);
    }

    return switch (method) {
      case "setup" -> jsonrpcSuccess(id, methodSetup());
      case "create_tables" -> jsonrpcSuccess(id, methodCreateTables());
      case "teardown" -> jsonrpcSuccess(id, methodTeardown());
      case "metrics" -> jsonrpcSuccess(id, methodMetrics());
      case "rpc.methods" -> jsonrpcSuccess(id, methodRpcMethods());
      default -> jsonrpcError(id, -32601, "Method not found", null);
    };
  }

  private static ObjectNode jsonrpcSuccess(JsonNode id, JsonNode result) {
    ObjectNode response = MAPPER.createObjectNode();
    response.put("jsonrpc", JSONRPC_VERSION);
    response.set("id", id == null ? MAPPER.nullNode() : id);
    response.set("result", result);
    return response;
  }

  private static ObjectNode jsonrpcError(JsonNode id, int code, String message, Object data) {
    ObjectNode response = MAPPER.createObjectNode();
    response.put("jsonrpc", JSONRPC_VERSION);
    response.set("id", id == null ? MAPPER.nullNode() : id);

    ObjectNode error = MAPPER.createObjectNode();
    error.put("code", code);
    error.put("message", message);
    if (data != null) {
      error.set("data", MAPPER.valueToTree(data));
    }

    response.set("error", error);
    return response;
  }

  private static JsonNode methodSetup() {
    // Stub: Provision or initialize your SUT for this run and return
    // query driver details SpiceBench should use.
    // Example:
    // - create run-scoped schema/database
    // - configure ingestion resources for datasets
    // - wait for readiness checks to pass
    // - resolve endpoint and credentials from your control plane
    String host = getenvOr("SUT_HOST", "127.0.0.1");
    int port = getenvIntOr("SUT_PORT", 50051);
    boolean tls = "true".equalsIgnoreCase(getenvOr("SUT_TLS", "false"));

    ObjectNode result = MAPPER.createObjectNode();
    result.put("driver", "flightsql");

    ObjectNode dbKwargs = MAPPER.createObjectNode();
    dbKwargs.put("uri", String.format("grpc%s://%s:%d", tls ? "s" : "", host, port));
    dbKwargs.put("username", getenvOr("SUT_USERNAME", ""));
    dbKwargs.put("password", getenvOr("SUT_PASSWORD", ""));
    dbKwargs.put("tls", tls);

    result.set("db_kwargs", dbKwargs);
    return result;
  }

  private static JsonNode methodCreateTables() {
    // Stub: Create/register destination tables for benchmark datasets.
    // Example:
    // - create tables if they do not exist
    // - apply expected schema/partitioning
    ObjectNode result = MAPPER.createObjectNode();
    result.put("ok", true);
    return result;
  }

  private static JsonNode methodTeardown() {
    // Stub: Deprovision resources created in setup.
    // Example:
    // - drop run-scoped schema/database
    // - stop ingestion workers/jobs
    ObjectNode result = MAPPER.createObjectNode();
    result.put("ok", true);
    return result;
  }

  private static JsonNode methodMetrics() {
    // Stub: Poll live SUT telemetry and map to this metrics schema.
    // Example sources:
    // - CPU/memory/disk from host/cloud monitoring APIs
    // - ingestion throughput from ingestion status endpoint
    // - active connections from DB/service diagnostics endpoint
    ObjectNode resource = MAPPER.createObjectNode();
    resource.put("cpu_usage_percent", 0.0);
    resource.put("memory_usage_bytes", 0);
    resource.put("disk_read_bytes", 0);
    resource.put("disk_write_bytes", 0);
    resource.put("disk_read_iops", 0);
    resource.put("disk_write_iops", 0);

    ObjectNode ingestion = MAPPER.createObjectNode();
    ingestion.put("rows_ingested", 0);
    ingestion.put("bytes_ingested", 0);
    ingestion.put("rows_per_sec", 0.0);
    ingestion.put("active_connections", 0);

    ObjectNode result = MAPPER.createObjectNode();
    result.set("resource", resource);
    result.set("ingestion", ingestion);
    return result;
  }

  private static JsonNode methodRpcMethods() {
    ArrayNode methods = MAPPER.createArrayNode();
    methods.add("setup");
    methods.add("create_tables");
    methods.add("teardown");
    methods.add("metrics");
    methods.add("rpc.methods");

    ObjectNode result = MAPPER.createObjectNode();
    result.set("methods", methods);
    return result;
  }

  private static String getenvOr(String key, String fallback) {
    return System.getenv().getOrDefault(key, fallback);
  }

  private static int getenvIntOr(String key, int fallback) {
    String value = System.getenv(key);
    if (value == null) {
      return fallback;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      return fallback;
    }
  }
}
