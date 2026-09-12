package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// newUUID generates a random UUID v4 using crypto/rand (no external dependency).
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:]),
	)
}

// ─────────────────────────────────────────────────────────────────────────────
// Backend struct: tracks a single backend's health and metrics
// ─────────────────────────────────────────────────────────────────────────────

type HealthData struct {
	Status            string  `json:"status"`
	CPUPercent        float64 `json:"cpu_percent"`
	MemoryPercent     float64 `json:"memory_percent"`
	ActiveConnections int     `json:"active_connections"`
	AvgResponseMs     float64 `json:"avg_response_ms"`
	MessageCount      int     `json:"message_count"`
	LoadScore         float64 `json:"load_score"`
}

type Backend struct {
	URL             string
	Alive           bool
	ConsecutiveFail int
	CooldownUntil   time.Time
	Health          HealthData
	TotalServed     int64
	TotalErrors     int64
	mux             sync.RWMutex
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if !alive {
		b.ConsecutiveFail++
		if b.ConsecutiveFail >= 3 {
			b.CooldownUntil = time.Now().Add(60 * time.Second)
			log.Printf("[CIRCUIT BREAKER] %s in cooldown for 60s", b.URL)
		}
	} else {
		b.ConsecutiveFail = 0
		b.CooldownUntil = time.Time{}
	}
	b.Alive = alive
}

func (b *Backend) IsAvailable() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	if !b.Alive {
		if !b.CooldownUntil.IsZero() && time.Now().After(b.CooldownUntil) {
			return true // Allow retry after cooldown
		}
		return false
	}
	return true
}

func (b *Backend) GetLoadScore() float64 {
	b.mux.RLock()
	defer b.mux.RUnlock()
	if !b.Alive {
		return math.MaxFloat64
	}
	return b.Health.LoadScore
}

func (b *Backend) UpdateHealth(h HealthData) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Health = h
}

// ─────────────────────────────────────────────────────────────────────────────
// ServerPool: manages all backends and routing decisions
// ─────────────────────────────────────────────────────────────────────────────

type ServerPool struct {
	backends  []*Backend
	threshold float64
	mu        sync.RWMutex
}

// SelectBest returns the backend with the lowest load score, below threshold.
// Falls back to the least-loaded backend if all are above threshold.
func (s *ServerPool) SelectBest() *Backend {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best *Backend
	bestScore := math.MaxFloat64

	var overloaded *Backend
	overloadedScore := math.MaxFloat64

	for _, b := range s.backends {
		if !b.IsAvailable() {
			continue
		}
		score := b.GetLoadScore()
		if score <= s.threshold {
			if score < bestScore {
				bestScore = score
				best = b
			}
		} else {
			if score < overloadedScore {
				overloadedScore = score
				overloaded = b
			}
		}
	}

	if best != nil {
		return best
	}
	if overloaded != nil {
		log.Printf("[WARN] All backends exceed threshold %.1f. Using least-loaded: %s (%.1f)",
			s.threshold, overloaded.URL, overloadedScore)
		return overloaded
	}
	return nil
}

// GetAvailable returns all currently alive/available backends.
func (s *ServerPool) GetAvailable() []*Backend {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var available []*Backend
	for _, b := range s.backends {
		if b.IsAvailable() {
			available = append(available, b)
		}
	}
	return available
}

// AdaptThreshold auto-adjusts the threshold based on observed load scores.
func (s *ServerPool) AdaptThreshold() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var scores []float64
	for _, b := range s.backends {
		if b.Alive && b.Health.LoadScore > 0 {
			scores = append(scores, b.Health.LoadScore)
		}
	}
	if len(scores) == 0 {
		return
	}
	var sum float64
	for _, sc := range scores {
		sum += sc
	}
	avg := sum / float64(len(scores))
	newThreshold := avg * 1.5
	if newThreshold < 30 {
		newThreshold = 30
	}
	if newThreshold > 85 {
		newThreshold = 85
	}
	s.threshold = newThreshold
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared HTTP client with high connection pool for concurrency
// ─────────────────────────────────────────────────────────────────────────────

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        5000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     60 * time.Second,
	},
}

// ─────────────────────────────────────────────────────────────────────────────
// Health Checking
// ─────────────────────────────────────────────────────────────────────────────

func healthCheck(pool *ServerPool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		pool.mu.RLock()
		backends := append([]*Backend(nil), pool.backends...)
		pool.mu.RUnlock()

		for _, b := range backends {
			go func(backend *Backend) {
				resp, err := httpClient.Get(backend.URL + "/health")
				if err != nil {
					wasAlive := backend.Alive
					backend.SetAlive(false)
					if wasAlive {
						log.Printf("[HEALTH] Backend %s OFFLINE: %v", backend.URL, err)
					}
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					backend.SetAlive(false)
					return
				}

				var h HealthData
				if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
					backend.SetAlive(false)
					return
				}

				wasAlive := backend.Alive
				backend.SetAlive(true)
				backend.UpdateHealth(h)
				if !wasAlive {
					log.Printf("[HEALTH] Backend %s ONLINE (score: %.1f)", backend.URL, h.LoadScore)
				}
			}(b)
		}
		pool.AdaptThreshold()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out write: writes to all healthy backends
// Returns as soon as the FIRST backend confirms write (fastest client response),
// while background goroutines replicate to the remaining backends.
// ─────────────────────────────────────────────────────────────────────────────

func fanOutWrite(pool *ServerPool, msgID, clientName, msgText string) (int, error) {
	backends := pool.GetAvailable()
	if len(backends) == 0 {
		return 0, fmt.Errorf("no available backends")
	}

	payload := url.Values{
		"client-name": {clientName},
		"msg":         {msgText},
		"msg_id":      {msgID},
	}

	firstSuccess := make(chan struct{}, 1)
	var replicatedCount int64
	var lastErr error
	var errMu sync.Mutex

	for _, b := range backends {
		go func(backend *Backend) {
			resp, err := httpClient.PostForm(backend.URL+"/message", payload)
			if err != nil {
				atomic.AddInt64(&backend.TotalErrors, 1)
				errMu.Lock()
				lastErr = err
				errMu.Unlock()
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)

			if resp.StatusCode >= 400 {
				atomic.AddInt64(&backend.TotalErrors, 1)
				errMu.Lock()
				lastErr = fmt.Errorf("status %d", resp.StatusCode)
				errMu.Unlock()
				return
			}

			atomic.AddInt64(&backend.TotalServed, 1)
			count := atomic.AddInt64(&replicatedCount, 1)
			if count == 1 {
				select {
				case firstSuccess <- struct{}{}:
				default:
				}
			}
		}(b)
	}

	select {
	case <-firstSuccess:
		return int(atomic.LoadInt64(&replicatedCount)), nil
	case <-time.After(8 * time.Second):
		count := int(atomic.LoadInt64(&replicatedCount))
		if count > 0 {
			return count, nil
		}
		errMu.Lock()
		defer errMu.Unlock()
		if lastErr != nil {
			return 0, lastErr
		}
		return 0, fmt.Errorf("all writes failed or timed out")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Request counters
// ─────────────────────────────────────────────────────────────────────────────

var (
	totalRequests int64
	totalErrors   int64
)

// ─────────────────────────────────────────────────────────────────────────────
// HTTP Handler
// ─────────────────────────────────────────────────────────────────────────────

func makeHandler(pool *ServerPool) http.Handler {
	mux := http.NewServeMux()

	// ── POST /message ────────────────────────────────────────────────────────
	mux.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)
		t0 := time.Now()

		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var clientName, msgText string
		bodyBytes, _ := io.ReadAll(r.Body)

		// 1. Try JSON decode
		var jsonBody map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &jsonBody); err == nil && len(jsonBody) > 0 {
			if v, ok := jsonBody["client-name"]; ok {
				clientName = strings.TrimSpace(fmt.Sprint(v))
			} else if v, ok := jsonBody["username"]; ok {
				clientName = strings.TrimSpace(fmt.Sprint(v))
			}
			if v, ok := jsonBody["msg"]; ok {
				msgText = strings.TrimSpace(fmt.Sprint(v))
			} else if v, ok := jsonBody["text"]; ok {
				msgText = strings.TrimSpace(fmt.Sprint(v))
			}
		}

		// 2. If not found, try Form URL-encoded decode
		if clientName == "" || msgText == "" {
			vals, err := url.ParseQuery(string(bodyBytes))
			if err == nil && len(vals) > 0 {
				if clientName == "" {
					clientName = strings.TrimSpace(vals.Get("client-name"))
					if clientName == "" {
						clientName = strings.TrimSpace(vals.Get("username"))
					}
				}
				if msgText == "" {
					msgText = strings.TrimSpace(vals.Get("msg"))
					if msgText == "" {
						msgText = strings.TrimSpace(vals.Get("text"))
					}
				}
			}
		}

		if clientName == "" {
			http.Error(w, `{"error":"client-name is required"}`, http.StatusBadRequest)
			return
		}
		if msgText == "" {
			http.Error(w, `{"error":"msg is required"}`, http.StatusBadRequest)
			return
		}

		// Generate unique ID at the Load Balancer to coordinate all backends
		msgID := newUUID()

		success, err := fanOutWrite(pool, msgID, clientName, msgText)
		if success == 0 {
			atomic.AddInt64(&totalErrors, 1)
			log.Printf("[ERROR] All backends failed: %v", err)
			http.Error(w, `{"error":"all backends unavailable"}`, http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"msg_id":      msgID,
			"client-name": clientName,
			"msg":         msgText,
			"replicated":  success,
			"latency_ms":  time.Since(t0).Milliseconds(),
		})
	})

	// ── GET /feed ─────────────────────────────────────────────────────────────
	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		// Read from the best (lowest load) available backend
		target := pool.SelectBest()
		if target == nil {
			atomic.AddInt64(&totalErrors, 1)
			http.Error(w, `{"error":"no available backends"}`, http.StatusServiceUnavailable)
			return
		}

		resp, err := httpClient.Get(target.URL + "/feed")
		if err != nil || resp.StatusCode != http.StatusOK {
			// Try other backends as fallback
			for _, b := range pool.GetAvailable() {
				if b.URL == target.URL {
					continue
				}
				resp, err = httpClient.Get(b.URL + "/feed")
				if err == nil && resp.StatusCode == http.StatusOK {
					break
				}
			}
		}
		if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
			atomic.AddInt64(&totalErrors, 1)
			if resp != nil {
				resp.Body.Close()
			}
			http.Error(w, `{"error":"feed unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	})

	// ── GET /lb-stats ─────────────────────────────────────────────────────────
	mux.HandleFunc("/lb-stats", func(w http.ResponseWriter, r *http.Request) {
		pool.mu.RLock()
		defer pool.mu.RUnlock()

		stats := make([]map[string]interface{}, 0, len(pool.backends))
		for _, b := range pool.backends {
			b.mux.RLock()
			stats = append(stats, map[string]interface{}{
				"url":          b.URL,
				"alive":        b.Alive,
				"load_score":   b.Health.LoadScore,
				"cpu_percent":  b.Health.CPUPercent,
				"memory_pct":   b.Health.MemoryPercent,
				"active_conns": b.Health.ActiveConnections,
				"avg_resp_ms":  b.Health.AvgResponseMs,
				"msg_count":    b.Health.MessageCount,
				"total_served": b.TotalServed,
				"total_errors": b.TotalErrors,
				"consec_fail":  b.ConsecutiveFail,
			})
			b.mux.RUnlock()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"threshold":      pool.threshold,
			"total_requests": atomic.LoadInt64(&totalRequests),
			"total_errors":   atomic.LoadInt64(&totalErrors),
			"backends":       stats,
		})
	})

	// ── GET /health (LB itself) ───────────────────────────────────────────────
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
			"role":   "load-balancer",
		})
	})

	return mux
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	port := flag.Int("port", 3210, "Load Balancer listening port")
	backendsStr := flag.String("backends",
		"http://172.17.0.11:4210,http://172.17.0.12:3211,http://172.17.0.13:3212",
		"Comma-separated backend base URLs")
	thresholdFlag := flag.Float64("threshold", 60.0, "Initial load score threshold (0-100)")
	flag.Parse()

	pool := &ServerPool{
		threshold: *thresholdFlag,
	}

	for _, rawURL := range strings.Split(*backendsStr, ",") {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		pool.backends = append(pool.backends, &Backend{
			URL:   rawURL,
			Alive: true,
		})
		log.Printf("[INIT] Backend registered: %s", rawURL)
	}

	// Start background health checker
	go healthCheck(pool, 2*time.Second)

	// Brief pause so initial health checks populate metrics
	time.Sleep(1 * time.Second)

	server := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", *port),
		Handler:      makeHandler(pool),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("==========================================")
	log.Printf("  Lab 6 Dynamic Load Balancer")
	log.Printf("  Listening: http://0.0.0.0:%d", *port)
	log.Printf("  Threshold: %.0f  |  Backends: %d", pool.threshold, len(pool.backends))
	log.Printf("  Routes: POST /message  GET /feed  GET /lb-stats")
	log.Printf("==========================================")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
