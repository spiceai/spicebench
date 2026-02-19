package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const jsonrpcVersion = "2.0"

type jsonRpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type jsonRpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type jsonRpcResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      interface{}   `json:"id"`
	Result  interface{}   `json:"result,omitempty"`
	Error   *jsonRpcError `json:"error,omitempty"`
}

func parseID(raw json.RawMessage) interface{} {
	if len(raw) == 0 {
		return nil
	}
	var id interface{}
	if err := json.Unmarshal(raw, &id); err != nil {
		return nil
	}
	return id
}

func success(id interface{}, result interface{}) jsonRpcResponse {
	return jsonRpcResponse{JSONRPC: jsonrpcVersion, ID: id, Result: result}
}

func failure(id interface{}, code int, message string, data interface{}) jsonRpcResponse {
	return jsonRpcResponse{
		JSONRPC: jsonrpcVersion,
		ID:      id,
		Error: &jsonRpcError{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
}

func methodSetup(_ map[string]interface{}) interface{} {
	// Stub: Provision or initialize your SUT for this run and return
	// query driver details SpiceBench should use.
	// Example:
	// - create run-scoped database/schema
	// - configure ingestion resources for dataset list
	// - resolve query endpoint and auth material from control plane

	host := getenvOr("SUT_HOST", "127.0.0.1")
	port := getenvIntOr("SUT_PORT", 50051)
	tls := strings.EqualFold(getenvOr("SUT_TLS", "false"), "true")

	scheme := "grpc"
	if tls {
		scheme = "grpcs"
	}

	return map[string]interface{}{
		"driver": "flightsql",
		"db_kwargs": map[string]interface{}{
			"uri":      fmt.Sprintf("%s://%s:%d", scheme, host, port),
			"username": getenvOr("SUT_USERNAME", ""),
			"password": getenvOr("SUT_PASSWORD", ""),
			"tls":      tls,
		},
	}
}

func methodCreateTables(_ map[string]interface{}) interface{} {
	// Stub: Create/register destination tables for benchmark datasets.
	// Example:
	// - create tables if they do not exist
	// - apply expected schema/partitioning
	return map[string]interface{}{"ok": true}
}

func methodTeardown(_ map[string]interface{}) interface{} {
	// Stub: Clean up resources created during setup.
	// Example:
	// - drop run-scoped database/schema
	// - terminate ingestion workers/jobs
	return map[string]interface{}{"ok": true}
}

func methodMetrics(_ map[string]interface{}) interface{} {
	// Stub: Poll live SUT telemetry and translate to this metrics schema.
	// Example sources:
	// - CPU/memory/disk metrics from host/cloud monitoring APIs
	// - ingestion throughput from ingestion status endpoint
	// - active connections from DB/service diagnostics APIs
	return map[string]interface{}{
		"resource": map[string]interface{}{
			"cpu_usage_percent":  0.0,
			"memory_usage_bytes": 0,
			"disk_read_bytes":    0,
			"disk_write_bytes":   0,
			"disk_read_iops":     0,
			"disk_write_iops":    0,
		},
		"ingestion": map[string]interface{}{
			"rows_ingested":      0,
			"bytes_ingested":     0,
			"rows_per_sec":       0.0,
			"active_connections": 0,
		},
	}
}

func methodRpcMethods() interface{} {
	return map[string]interface{}{
		"methods": []string{"setup", "create_tables", "teardown", "metrics", "rpc.methods"},
	}
}

func dispatch(request []byte) jsonRpcResponse {
	var req jsonRpcRequest
	if err := json.Unmarshal(request, &req); err != nil {
		return failure(nil, -32700, "Parse error", err.Error())
	}

	id := parseID(req.ID)

	if req.JSONRPC != jsonrpcVersion {
		return failure(id, -32600, "Invalid Request: jsonrpc must be '2.0'", nil)
	}

	if req.Method == "" {
		return failure(id, -32600, "Invalid Request: method must be a string", nil)
	}

	params := map[string]interface{}{}
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return failure(id, -32602, "Invalid params: expected object", err.Error())
		}
	}

	switch req.Method {
	case "setup":
		return success(id, methodSetup(params))
	case "create_tables":
		return success(id, methodCreateTables(params))
	case "teardown":
		return success(id, methodTeardown(params))
	case "metrics":
		return success(id, methodMetrics(params))
	case "rpc.methods":
		return success(id, methodRpcMethods())
	default:
		return failure(id, -32601, "Method not found", nil)
	}
}

func writeResponse(w io.Writer, response jsonRpcResponse) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(response)
}

func runStdio() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		response := dispatch(line)
		if err := writeResponse(os.Stdout, response); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func runHTTP(host string, port int, path string) error {
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			response := failure(nil, -32603, "Internal error", err.Error())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(response)
			return
		}

		response := dispatch(body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})

	address := fmt.Sprintf("%s:%d", host, port)
	log.Printf("JSON-RPC HTTP server listening on http://%s%s", address, path)
	return http.ListenAndServe(address, mux)
}

func getenvOr(key string, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return value
}

func getenvIntOr(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func main() {
	transport := flag.String("transport", "stdio", "Transport: stdio or http")
	host := flag.String("host", "127.0.0.1", "Host for HTTP transport")
	port := flag.Int("port", 8080, "Port for HTTP transport")
	path := flag.String("path", "/jsonrpc", "HTTP path for JSON-RPC")
	flag.Parse()

	var err error
	switch *transport {
	case "stdio":
		err = runStdio()
	case "http":
		err = runHTTP(*host, *port, *path)
	default:
		err = fmt.Errorf("invalid --transport value: %s", *transport)
	}

	if err != nil {
		log.Fatal(err)
	}
}
