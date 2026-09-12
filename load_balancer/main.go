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
	mrand "math/rand"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// newUUID generates a random UUID v4 using crypto/rand (no external dependency).
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:]),
	)
}

// ─────────────────────────────────────────────────────────────────────────────
// Backend struct: tracks health and dynamic metrics
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
	ActiveInFlight  int64
	mux             sync.RWMutex
}

func (b *Backend) RecordFailure() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.ConsecutiveFail++
	if b.ConsecutiveFail >= 3 {
		b.Alive = false
		b.CooldownUntil = time.Now().Add(5 * time.Second)
	}
}

func (b *Backend) RecordSuccess() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.ConsecutiveFail = 0
	b.Alive = true
	b.CooldownUntil = time.Time{}
}

func (b *Backend) SetAlive(alive bool) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if !alive {
		b.ConsecutiveFail++
		if b.ConsecutiveFail >= 2 {
			b.Alive = false
			b.CooldownUntil = time.Now().Add(3 * time.Second)
		}
	} else {
		b.ConsecutiveFail = 0
		b.Alive = true
		b.CooldownUntil = time.Time{}
	}
}

func (b *Backend) IsAvailable() bool {
	b.mux.RLock()
	defer b.mux.RUnlock()
	if !b.Alive {
		if !b.CooldownUntil.IsZero() && time.Now().After(b.CooldownUntil) {
			return true
		}
		return false
	}
	return true
}

func (b *Backend) GetEffectiveScore() float64 {
	b.mux.RLock()
	defer b.mux.RUnlock()
	if !b.Alive {
		return math.MaxFloat64
	}
	inflight := atomic.LoadInt64(&b.ActiveInFlight)
	// Base load score + 2.0 penalty per concurrent in-flight request
	return b.Health.LoadScore + float64(inflight)*2.0
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
// ServerPool: dynamic performance-based routing
// ─────────────────────────────────────────────────────────────────────────────

type ServerPool struct {
	backends  []*Backend
	threshold float64
	mu        sync.RWMutex
}

// SelectBest picks the backend with lowest effective load score (health + in-flight).
// If all exceed threshold, picks least loaded.
// Never returns nil if any backend exists.
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
		score := b.GetEffectiveScore()
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
		return overloaded
	}
	// Fallback: if all marked down, return random backend to retry
	if len(s.backends) > 0 {
		return s.backends[mrand.Intn(len(s.backends))]
	}
	return nil
}

func (s *ServerPool) GetAll() []*Backend {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]*Backend(nil), s.backends...)
}

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
// HTTP Client optimized for 1,000+ concurrent connections
// ─────────────────────────────────────────────────────────────────────────────

var httpClient = &http.Client{
	Timeout: 1500 * time.Millisecond,
	Transport: &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	},
}

var feedHttpClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     60 * time.Second,
	},
}

// ─────────────────────────────────────────────────────────────────────────────
// Background Replication Queue
// Replicates messages asynchronously to peer backends without slowing clients.
// ─────────────────────────────────────────────────────────────────────────────

type ReplicationTask struct {
	BackendURL string
	Payload    url.Values
}

var replicationCh = make(chan ReplicationTask, 50000)

func startReplicationWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func() {
			for task := range replicationCh {
				resp, err := httpClient.PostForm(task.BackendURL+"/message", task.Payload)
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Health Checking
// ─────────────────────────────────────────────────────────────────────────────

func healthCheck(pool *ServerPool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		backends := pool.GetAll()
		for _, b := range backends {
			go func(backend *Backend) {
				resp, err := httpClient.Get(backend.URL + "/health")
				if err != nil || resp.StatusCode != http.StatusOK {
					if resp != nil {
						resp.Body.Close()
					}
					backend.SetAlive(false)
					return
				}
				defer resp.Body.Close()

				var h HealthData
				if err := json.NewDecoder(resp.Body).Decode(&h); err == nil {
					backend.SetAlive(true)
					backend.UpdateHealth(h)
				}
			}(b)
		}
		pool.AdaptThreshold()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Route message to best backend, then queue async replication
// ─────────────────────────────────────────────────────────────────────────────

func routeMessage(pool *ServerPool, msgID, clientName, msgText string) (string, error) {
	target := pool.SelectBest()
	if target == nil {
		return "", fmt.Errorf("no backends available")
	}

	atomic.AddInt64(&target.ActiveInFlight, 1)
	defer atomic.AddInt64(&target.ActiveInFlight, -1)

	payload := url.Values{
		"client-name": {clientName},
		"msg":         {msgText},
		"msg_id":      {msgID},
	}

	// Try target backend first
	resp, err := httpClient.PostForm(target.URL+"/message", payload)
	if err != nil || resp.StatusCode >= 400 {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		target.RecordFailure()
		atomic.AddInt64(&target.TotalErrors, 1)

		// Fallback: try any other backend
		backends := pool.GetAll()
		for _, alt := range backends {
			if alt.URL == target.URL {
				continue
			}
			resp, err = httpClient.PostForm(alt.URL+"/message", payload)
			if err == nil && resp.StatusCode < 400 {
				target = alt
				break
			}
			if resp != nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}

	if err != nil || resp == nil || resp.StatusCode >= 400 {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return "", fmt.Errorf("backend rejected message")
	}

	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	target.RecordSuccess()
	atomic.AddInt64(&target.TotalServed, 1)

	return target.URL, nil
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

type FeedMessage struct {
	ID         string `json:"id"`
	ClientName string `json:"client-name"`
	Msg        string `json:"msg"`
	Timestamp  int64  `json:"timestamp"`
}

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
				clientName = fmt.Sprint(v)
			} else if v, ok := jsonBody["username"]; ok {
				clientName = fmt.Sprint(v)
			}
			if v, ok := jsonBody["msg"]; ok {
				msgText = fmt.Sprint(v)
			} else if v, ok := jsonBody["text"]; ok {
				msgText = fmt.Sprint(v)
			}
		}

		// 2. Try Form URL-encoded
		if clientName == "" || msgText == "" {
			vals, err := url.ParseQuery(string(bodyBytes))
			if err == nil && len(vals) > 0 {
				if clientName == "" {
					clientName = vals.Get("client-name")
					if clientName == "" {
						clientName = vals.Get("username")
					}
				}
				if msgText == "" {
					msgText = vals.Get("msg")
					if msgText == "" {
						msgText = vals.Get("text")
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

		msgID := newUUID()
		backendURL, err := routeMessage(pool, msgID, clientName, msgText)
		if err != nil {
			atomic.AddInt64(&totalErrors, 1)
			http.Error(w, `{"error":"service unavailable"}`, http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"msg_id":      msgID,
			"client-name": clientName,
			"msg":         msgText,
			"backend":     backendURL,
			"latency_ms":  time.Since(t0).Milliseconds(),
		})
	})

	// ── GET /feed (Merges all backends for 100% completeness) ─────────────────
	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		backends := pool.GetAll()
		type feedResult struct {
			msgs []FeedMessage
			err  error
		}

		ch := make(chan feedResult, len(backends))

		for _, b := range backends {
			go func(backendURL string) {
				resp, err := feedHttpClient.Get(backendURL + "/feed")
				if err != nil || resp.StatusCode != http.StatusOK {
					if resp != nil {
						resp.Body.Close()
					}
					ch <- feedResult{nil, fmt.Errorf("failed")}
					return
				}
				defer resp.Body.Close()
				var msgs []FeedMessage
				json.NewDecoder(resp.Body).Decode(&msgs)
				ch <- feedResult{msgs, nil}
			}(b.URL)
		}

		// Collect and merge messages from all backends
		seen := make(map[string]bool)
		merged := make([]FeedMessage, 0)

		for range backends {
			res := <-ch
			if res.err == nil && res.msgs != nil {
				for _, m := range res.msgs {
					key := m.ID
					if key == "" {
						key = fmt.Sprintf("%s_%s_%d", m.ClientName, m.Msg, m.Timestamp)
					}
					if !seen[key] {
						seen[key] = true
						merged = append(merged, m)
					}
				}
			}
		}

		// Sort merged messages by timestamp ascending
		sort.Slice(merged, func(i, j int) bool {
			return merged[i].Timestamp < merged[j].Timestamp
		})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(merged)
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
	port := flag.Int("port", 3000, "Load Balancer listening port")
	backendsStr := flag.String("backends",
		"http://172.17.0.11:4000,http://172.17.0.12:3000,http://172.17.0.13:3000",
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

	time.Sleep(1 * time.Second)

	handler := makeHandler(pool)

	// Ensure LB listens on BOTH port 3000 and port 3210
	go func() {
		sDual := &http.Server{
			Addr:         "0.0.0.0:3210",
			Handler:      handler,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		}
		log.Printf("[DUAL] Also listening on http://0.0.0.0:3210")
		if err := sDual.ListenAndServe(); err != nil {
			log.Printf("[DUAL] Port 3210 listener: %v", err)
		}
	}()

	server := &http.Server{
		Addr:         "0.0.0.0:3000",
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("==========================================")
	log.Printf("  Lab 6 High-Performance Dynamic Load Balancer")
	log.Printf("  Listening: http://0.0.0.0:%d and http://0.0.0.0:3000", *port)
	log.Printf("  Threshold: %.0f  |  Backends: %d", pool.threshold, len(pool.backends))
	log.Printf("  Routes: POST /message  GET /feed  GET /lb-stats")
	log.Printf("==========================================")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
