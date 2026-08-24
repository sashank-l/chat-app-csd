package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Backend represents a single chat server instance (Sys2, Sys3, or Sys4).
type Backend struct {
	URL          *url.URL
	Alive        bool
	ReverseProxy *httputil.ReverseProxy
	ActiveConns  int64
	TotalServed  int64
	mux          sync.RWMutex
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Alive = alive
}

func (b *Backend) IsAlive() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	return b.Alive
}

func (b *Backend) IncrConn() {
	atomic.AddInt64(&b.ActiveConns, 1)
	atomic.AddInt64(&b.TotalServed, 1)
}

func (b *Backend) DecrConn() {
	atomic.AddInt64(&b.ActiveConns, -1)
}

func (b *Backend) GetActiveConns() int64 {
	return atomic.LoadInt64(&b.ActiveConns)
}

func (b *Backend) GetTotalServed() int64 {
	return atomic.LoadInt64(&b.TotalServed)
}

// ServerPool maintains the pool of backends and routes traffic.
type ServerPool struct {
	backends []*Backend
	current  uint64
	algo     string // "leastconn" or "roundrobin"
	mux      sync.RWMutex
}

func (s *ServerPool) AddBackend(backend *Backend) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.backends = append(s.backends, backend)
}

// NextBackend selects the next available healthy backend based on the configured algorithm.
func (s *ServerPool) NextBackend() *Backend {
	s.mux.RLock()
	defer s.mux.RUnlock()

	var healthy []*Backend
	for _, b := range s.backends {
		if b.IsAlive() {
			healthy = append(healthy, b)
		}
	}

	if len(healthy) == 0 {
		return nil
	}

	if s.algo == "leastconn" {
		least := healthy[0]
		minConns := least.GetActiveConns()
		for _, b := range healthy[1:] {
			if conns := b.GetActiveConns(); conns < minConns {
				least = b
				minConns = conns
			}
		}
		return least
	}

	// Default: Round Robin
	idx := atomic.AddUint64(&s.current, 1) % uint64(len(healthy))
	return healthy[idx]
}

// HealthCheck periodically pings /health on all backends.
func (s *ServerPool) HealthCheck(interval time.Duration) {
	client := http.Client{
		Timeout: 2 * time.Second,
	}

	for range time.Tick(interval) {
		s.mux.RLock()
		backends := append([]*Backend(nil), s.backends...)
		s.mux.RUnlock()

		for _, b := range backends {
			healthURL := b.URL.String() + "/health"
			resp, err := client.Get(healthURL)
			alive := err == nil && resp.StatusCode == http.StatusOK
			if resp != nil {
				resp.Body.Close()
			}

			prev := b.IsAlive()
			b.SetAlive(alive)
			if prev != alive {
				if alive {
					log.Printf("🟢 [HEALTH] Backend %s is ONLINE", b.URL)
				} else {
					log.Printf("🔴 [HEALTH] Backend %s is OFFLINE (%v)", b.URL, err)
				}
			}
		}
	}
}

// isWebSocketRequest checks if an incoming HTTP request is a WebSocket upgrade handshake.
func isWebSocketRequest(r *http.Request) bool {
	containsHeader := func(header, value string) bool {
		for _, v := range strings.Split(header, ",") {
			if strings.EqualFold(strings.TrimSpace(v), value) {
				return true
			}
		}
		return false
	}
	return containsHeader(r.Header.Get("Connection"), "Upgrade") &&
		strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// proxyWebSocket performs full-duplex TCP streaming for WebSocket connections.
func proxyWebSocket(w http.ResponseWriter, r *http.Request, target *Backend) {
	target.IncrConn()
	defer target.DecrConn()

	targetHost := target.URL.Host

	// Connect to target backend TCP socket
	backendConn, err := net.DialTimeout("tcp", targetHost, 5*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to dial backend: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer backendConn.Close()

	// Hijack client TCP connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, fmt.Sprintf("Hijack failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	// Forward raw handshake request to backend preserving all WebSocket upgrade headers
	uri := r.URL.RequestURI()
	if uri == "" {
		uri = "/ws"
	}
	var reqBuilder strings.Builder
	reqBuilder.WriteString(fmt.Sprintf("%s %s HTTP/1.1\r\n", r.Method, uri))
	reqBuilder.WriteString(fmt.Sprintf("Host: %s\r\n", targetHost))

	hasConnection := false
	hasUpgrade := false

	for key, values := range r.Header {
		if strings.EqualFold(key, "Host") {
			continue
		}
		if strings.EqualFold(key, "Connection") {
			hasConnection = true
		}
		if strings.EqualFold(key, "Upgrade") {
			hasUpgrade = true
		}
		for _, value := range values {
			reqBuilder.WriteString(fmt.Sprintf("%s: %s\r\n", key, value))
		}
	}

	if !hasConnection {
		reqBuilder.WriteString("Connection: Upgrade\r\n")
	}
	if !hasUpgrade {
		reqBuilder.WriteString("Upgrade: websocket\r\n")
	}
	reqBuilder.WriteString("\r\n")

	if _, err := backendConn.Write([]byte(reqBuilder.String())); err != nil {
		log.Printf("Failed to forward WS handshake: %v", err)
		return
	}

	// Bi-directional full-duplex streaming
	errChan := make(chan error, 2)
	go func() {
		_, err := io.Copy(backendConn, clientBuf)
		errChan <- err
	}()
	go func() {
		_, err := io.Copy(clientConn, backendConn)
		errChan <- err
	}()

	<-errChan
}

func main() {
	port := flag.Int("port", 4209, "Load Balancer listening port (e.g. 4209 for Sys1)")
	backendsFlag := flag.String("backends", "http://172.17.0.11:4210,http://172.17.0.12:4211,http://172.17.0.13:4212", "Comma-separated backend URLs (Sys2, Sys3, Sys4)")
	mode := flag.String("mode", "all", "Routing mode: 'single' (only Sys2) or 'all' (Sys2 + Sys3 + Sys4)")
	algo := flag.String("algo", "leastconn", "Load balancing algorithm: 'leastconn' or 'roundrobin'")
	flag.Parse()

	backendURLs := strings.Split(*backendsFlag, ",")
	if *mode == "single" && len(backendURLs) > 0 {
		backendURLs = backendURLs[:1]
		log.Printf("⚡ Running in SINGLE BACKEND mode (Sys2 only): %v", backendURLs)
	} else {
		log.Printf("⚡ Running in MULTI BACKEND mode (Sys2 + Sys3 + Sys4): %v", backendURLs)
	}

	serverPool := &ServerPool{algo: *algo}

	for _, rawURL := range backendURLs {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
			rawURL = "http://" + rawURL
		}

		u, err := url.Parse(rawURL)
		if err != nil {
			log.Fatalf("Invalid backend URL %s: %v", rawURL, err)
		}

		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("Proxy error on %s: %v", u, err)
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprintf(w, "Bad Gateway: %v", err)
		}

		backend := &Backend{
			URL:          u,
			Alive:        true,
			ReverseProxy: proxy,
		}
		serverPool.AddBackend(backend)
		log.Printf("Registered Backend: %s", u)
	}

	// Start background health checking
	go serverPool.HealthCheck(2 * time.Second)

	// Main Load Balancer HTTP / WebSocket Handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Internal stats endpoint for metrics & grading report
		if r.URL.Path == "/lb-stats" {
			w.Header().Set("Content-Type", "application/json")
			serverPool.mux.RLock()
			stats := make([]map[string]interface{}, len(serverPool.backends))
			for i, b := range serverPool.backends {
				stats[i] = map[string]interface{}{
					"url":          b.URL.String(),
					"alive":        b.IsAlive(),
					"active_conns": b.GetActiveConns(),
					"total_served": b.GetTotalServed(),
				}
			}
			serverPool.mux.RUnlock()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"mode":      *mode,
				"algorithm": *algo,
				"backends":  stats,
			})
			return
		}

		// Select next backend
		target := serverPool.NextBackend()
		if target == nil {
			http.Error(w, "No healthy backend available", http.StatusServiceUnavailable)
			return
		}

		// Handle WebSocket upgrades
		if isWebSocketRequest(r) {
			log.Printf("🔀 [WS PROXY] Upgrading WebSocket -> %s (Active: %d)", target.URL, target.GetActiveConns()+1)
			proxyWebSocket(w, r, target)
			return
		}

		// Handle standard HTTP requests
		target.IncrConn()
		defer target.DecrConn()
		target.ReverseProxy.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", *port),
		Handler: handler,
	}

	log.Printf("==========================================================")
	log.Printf("🚀 Go Load Balancer running on http://0.0.0.0:%d", *port)
	log.Printf("🎯 Algorithm: %s | Mode: %s", *algo, *mode)
	log.Printf("📊 Live Metrics Endpoint: http://0.0.0.0:%d/lb-stats", *port)
	log.Printf("==========================================================")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
