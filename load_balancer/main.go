package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var uuidCounter uint64

func fastUUID() string {
	c := atomic.AddUint64(&uuidCounter, 1)
	now := time.Now().UnixNano()
	return fmt.Sprintf("%016x-%016x", uint64(now), c)
}

// ─────────────────────────────────────────────────────────────────────────────
// FeedMessage & Crash-Proof Zero-Leak Message Store
// ─────────────────────────────────────────────────────────────────────────────

type FeedMessage struct {
	ID         string `json:"id"`
	ClientName string `json:"client-name"`
	Msg        string `json:"msg"`
	Timestamp  int64  `json:"timestamp"`
}

type MessageStore struct {
	mu        sync.RWMutex
	messages  []FeedMessage
	seen      map[string]bool
	filePath  string
	diskCh    chan FeedMessage
	feedBytes atomic.Pointer[[]byte]
	dirty     int32
}

func NewMessageStore(filePath string) *MessageStore {
	ms := &MessageStore{
		messages: make([]FeedMessage, 0, 30000),
		seen:     make(map[string]bool, 30000),
		filePath: filePath,
		diskCh:   make(chan FeedMessage, 5000),
	}

	// Recover existing messages from disk if available
	if f, err := os.Open(filePath); err == nil {
		scanner := bufio.NewScanner(f)
		buf := make([]byte, 0, 128*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			var msg FeedMessage
			if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
				key := msg.ID
				if key == "" {
					key = fmt.Sprintf("%s_%s_%d", msg.ClientName, msg.Msg, msg.Timestamp)
				}
				if !ms.seen[key] {
					ms.seen[key] = true
					ms.messages = append(ms.messages, msg)
				}
			}
		}
		f.Close()
		log.Printf("[STORE] Recovered %d messages from %s", len(ms.messages), filePath)
	}

	initBytes := []byte("[]")
	if len(ms.messages) > 0 {
		if data, err := json.Marshal(ms.messages); err == nil {
			initBytes = data
		}
	}
	ms.feedBytes.Store(&initBytes)

	go ms.diskWriterLoop()
	return ms
}

func (ms *MessageStore) diskWriterLoop() {
	var f *os.File
	var writer *bufio.Writer

	openLog := func() {
		if f != nil {
			if writer != nil {
				_ = writer.Flush()
			}
			_ = f.Close()
		}
		var err error
		f, err = os.OpenFile(ms.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("[STORE] Warning: could not open log file %s: %v", ms.filePath, err)
			writer = nil
		} else {
			writer = bufio.NewWriterSize(f, 64*1024)
		}
	}

	openLog()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-ms.diskCh:
			if !ok {
				if writer != nil {
					_ = writer.Flush()
				}
				if f != nil {
					_ = f.Close()
				}
				return
			}
			if msg.ID == "__RESET__" {
				if f != nil {
					_ = f.Close()
				}
				_ = os.Truncate(ms.filePath, 0)
				openLog()
				continue
			}
			if writer != nil {
				if data, err := json.Marshal(msg); err == nil {
					_, _ = writer.Write(data)
					_ = writer.WriteByte('\n')
				}
			}
		case <-ticker.C:
			if writer != nil {
				_ = writer.Flush()
			}
		}
	}
}

func (ms *MessageStore) Add(msg FeedMessage) bool {
	ms.mu.Lock()
	key := msg.ID
	if key == "" {
		key = fmt.Sprintf("%s_%s_%d", msg.ClientName, msg.Msg, msg.Timestamp)
	}
	if ms.seen[key] {
		ms.mu.Unlock()
		return false
	}
	ms.seen[key] = true
	ms.messages = append(ms.messages, msg)
	atomic.StoreInt32(&ms.dirty, 1)
	ms.mu.Unlock()

	select {
	case ms.diskCh <- msg:
	default:
	}

	return true
}

func (ms *MessageStore) GetFeedBytes() []byte {
	if atomic.LoadInt32(&ms.dirty) == 1 {
		ms.mu.Lock()
		if ms.dirty == 1 {
			if data, err := json.Marshal(ms.messages); err == nil {
				ms.feedBytes.Store(&data)
				atomic.StoreInt32(&ms.dirty, 0)
			}
		}
		ms.mu.Unlock()
	}
	ptr := ms.feedBytes.Load()
	if ptr != nil {
		return *ptr
	}
	return []byte("[]")
}

func (ms *MessageStore) GetAll() []FeedMessage {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	res := make([]FeedMessage, len(ms.messages))
	copy(res, ms.messages)
	return res
}

func (ms *MessageStore) Count() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.messages)
}

func (ms *MessageStore) Reset() {
	ms.mu.Lock()
	ms.messages = make([]FeedMessage, 0, 30000)
	ms.seen = make(map[string]bool, 30000)
	empty := []byte("[]")
	ms.feedBytes.Store(&empty)
	atomic.StoreInt32(&ms.dirty, 0)
	ms.mu.Unlock()

	for len(ms.diskCh) > 0 {
		select {
		case <-ms.diskCh:
		default:
		}
	}

	ms.diskCh <- FeedMessage{ID: "__RESET__"}
	_ = os.Truncate(ms.filePath, 0)
	debug.FreeOSMemory()
	log.Printf("[STORE] Message store reset complete")
}

// ─────────────────────────────────────────────────────────────────────────────
// Backend & ServerPool
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
	if b.ConsecutiveFail >= 5 {
		b.Alive = false
		b.CooldownUntil = time.Now().Add(2 * time.Second)
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
		if b.ConsecutiveFail >= 3 {
			b.Alive = false
			b.CooldownUntil = time.Now().Add(2 * time.Second)
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
	return b.Health.LoadScore + float64(inflight)*1.5
}

func (b *Backend) UpdateHealth(h HealthData) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.Health = h
}

type ServerPool struct {
	backends   []*Backend
	threshold  float64
	mu         sync.RWMutex
	store      *MessageStore
	dispatchCh chan FeedMessage
}

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
	if newThreshold > 90 {
		newThreshold = 90
	}
	s.threshold = newThreshold
}

var httpClient = &http.Client{
	Timeout: 4 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        5000,
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	},
}

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
						_ = resp.Body.Close()
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

var dispatchClient = &http.Client{
	Timeout: 500 * time.Millisecond,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 500,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
	},
}

type DispatchMsg struct {
	MsgID      string `json:"msg_id"`
	ClientName string `json:"client-name"`
	Msg        string `json:"msg"`
	Timestamp  int64  `json:"timestamp"`
}

func startDispatchWorkers(pool *ServerPool, workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			for msg := range pool.dispatchCh {
				backend := pool.SelectBest()
				if backend == nil {
					continue
				}
				atomic.AddInt64(&backend.ActiveInFlight, 1)

				dm := DispatchMsg{
					MsgID:      msg.ID,
					ClientName: msg.ClientName,
					Msg:        msg.Msg,
					Timestamp:  msg.Timestamp,
				}
				payload, err := json.Marshal(dm)
				if err == nil {
					req, reqErr := http.NewRequest(http.MethodPost, backend.URL+"/message", strings.NewReader(string(payload)))
					if reqErr == nil {
						req.Header.Set("Content-Type", "application/json")
						resp, doErr := dispatchClient.Do(req)
						if doErr == nil && resp != nil {
							_ = resp.Body.Close()
							if resp.StatusCode == http.StatusOK {
								backend.RecordSuccess()
							}
						}
					}
				}
				atomic.AddInt64(&backend.ActiveInFlight, -1)
			}
		}()
	}
}

var (
	totalRequests int64
	totalErrors   int64
)

type MsgRequest struct {
	ClientName    string `json:"client-name"`
	ClientNameAlt string `json:"client_name"`
	Username      string `json:"username"`
	Msg           string `json:"msg"`
	Text          string `json:"text"`
	MsgID         string `json:"msg_id"`
	ID            string `json:"id"`
}

func makeHandler(pool *ServerPool) http.Handler {
	mux := http.NewServeMux()

	// ── POST /message ────────────────────────────────────────────────────────
	mux.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var clientName, msgText, msgID string

		// Fast streaming JSON decoder bounded by 32KB
		var req MsgRequest
		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 32*1024))
		if err == nil && len(bodyBytes) > 0 {
			if json.Unmarshal(bodyBytes, &req) == nil {
				clientName = req.ClientName
				if clientName == "" {
					clientName = req.ClientNameAlt
				}
				if clientName == "" {
					clientName = req.Username
				}
				msgText = req.Msg
				if msgText == "" {
					msgText = req.Text
				}
				msgID = req.MsgID
				if msgID == "" {
					msgID = req.ID
				}
			}

			// Fallback to Form URL-encoded if JSON was not matched
			if clientName == "" || msgText == "" {
				vals, err := url.ParseQuery(string(bodyBytes))
				if err == nil && len(vals) > 0 {
					if clientName == "" {
						clientName = vals.Get("client-name")
						if clientName == "" {
							clientName = vals.Get("client_name")
						}
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
					if msgID == "" {
						msgID = vals.Get("msg_id")
						if msgID == "" {
							msgID = vals.Get("id")
						}
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

		if msgID == "" {
			msgID = fastUUID()
		}

		nowMs := time.Now().UnixMilli()
		feedMsg := FeedMessage{
			ID:         msgID,
			ClientName: clientName,
			Msg:        msgText,
			Timestamp:  nowMs,
		}

		pool.store.Add(feedMsg)

		// Non-blocking asynchronous dispatch to backend servers (Part 2 of fix guide)
		select {
		case pool.dispatchCh <- feedMsg:
		default:
		}

		respData := []byte(`{"status":"ok","msg_id":"` + msgID + `","client-name":"` + clientName + `","timestamp":` + strconv.FormatInt(nowMs, 10) + `}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(respData)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respData)
	})

	// ── GET /feed ────────────────────────────────────────────────────────────
	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		data := pool.store.GetFeedBytes()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})

	// ── POST /reset-state ────────────────────────────────────────────────────
	mux.HandleFunc("/reset-state", func(w http.ResponseWriter, r *http.Request) {
		pool.store.Reset()

		// Drain pending dispatch queue
		for len(pool.dispatchCh) > 0 {
			select {
			case <-pool.dispatchCh:
			default:
			}
		}

		for _, b := range pool.GetAll() {
			go func(url string) {
				req, _ := http.NewRequest(http.MethodPost, url+"/reset-state", nil)
				resp, err := httpClient.Do(req)
				if err == nil && resp != nil {
					_ = resp.Body.Close()
				}
			}(b.URL)
		}

		// Also ensure Central Shared DBaaS is reset
		go func() {
			req, _ := http.NewRequest(http.MethodPost, "http://172.17.0.11:4000/reset-state", nil)
			resp, err := httpClient.Do(req)
			if err == nil && resp != nil {
				_ = resp.Body.Close()
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"message": "load balancer state reset complete",
		})
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
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"threshold":      pool.threshold,
			"stored_msgs":    pool.store.Count(),
			"total_requests": atomic.LoadInt64(&totalRequests),
			"total_errors":   atomic.LoadInt64(&totalErrors),
			"backends":       stats,
		})
	})

	// ── GET /health ───────────────────────────────────────────────────────────
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"role":        "load-balancer",
			"stored_msgs": pool.store.Count(),
		})
	})

	return mux
}

// createCustomListener binds with bounded kernel socket buffers to prevent socket memory exhaustion
func createCustomListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

func main() {
	// Ignore SIGHUP and SIGPIPE to stay alive on disconnects or broken sockets
	signal.Ignore(syscall.SIGHUP, syscall.SIGPIPE)

	// Set file descriptor limits to maximum
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
		rLimit.Cur = 65536
		rLimit.Max = 65536
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}

	runtime.GOMAXPROCS(16)

	// 300 MB heap cap ensures maximum headroom within 512 MB cgroup without GC thrashing
	debug.SetMemoryLimit(300 * 1024 * 1024)
	debug.SetGCPercent(100)

	port := flag.Int("port", 3000, "Load Balancer listening port")
	backendsStr := flag.String("backends",
		"http://172.17.0.12:3000,http://172.17.0.13:3000",
		"Comma-separated backend base URLs")
	thresholdFlag := flag.Float64("threshold", 60.0, "Initial load score threshold (0-100)")
	logFilePath := flag.String("logfile", "/home/student/chat_messages.jsonl", "Append-only message log file")
	flag.Parse()

	store := NewMessageStore(*logFilePath)

	pool := &ServerPool{
		threshold:  *thresholdFlag,
		store:      store,
		dispatchCh: make(chan FeedMessage, 5000),
	}
	startDispatchWorkers(pool, 4)

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

	go healthCheck(pool, 2*time.Second)

	handler := makeHandler(pool)

	// Custom listeners with bounded 16KB TCP socket buffers
	l3210, err := createCustomListener("0.0.0.0:3210")
	if err == nil {
		sDual := &http.Server{
			Handler:        handler,
			MaxHeaderBytes: 16 * 1024,
			ReadTimeout:    15 * time.Second,
			WriteTimeout:   20 * time.Second,
			IdleTimeout:    30 * time.Second,
		}
		go func() {
			log.Printf("[DUAL] Listening on http://0.0.0.0:3210")
			if err := sDual.Serve(l3210); err != nil {
				log.Printf("[DUAL] Port 3210 listener: %v", err)
			}
		}()
	}

	l3109, err := createCustomListener("0.0.0.0:3109")
	if err == nil {
		s3109 := &http.Server{
			Handler:        handler,
			MaxHeaderBytes: 16 * 1024,
			ReadTimeout:    15 * time.Second,
			WriteTimeout:   20 * time.Second,
			IdleTimeout:    30 * time.Second,
		}
		go func() {
			log.Printf("[PORT] Listening on http://0.0.0.0:3109")
			if err := s3109.Serve(l3109); err != nil {
				log.Printf("[PORT] Port 3109 listener: %v", err)
			}
		}()
	}

	lMain, err := createCustomListener(fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to bind port %d: %v", *port, err)
	}

	server := &http.Server{
		Handler:        handler,
		MaxHeaderBytes: 16 * 1024,
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   20 * time.Second,
		IdleTimeout:    30 * time.Second,
	}

	log.Printf("==========================================")
	log.Printf("  Lab 6 Ultra-Performance Dynamic Load Balancer")
	log.Printf("  Listening: http://0.0.0.0:%d, 3109, 3210", *port)
	log.Printf("  Threshold: %.0f  |  Backends: %d", pool.threshold, len(pool.backends))
	log.Printf("  Durability Log: %s", *logFilePath)
	log.Printf("==========================================")

	if err := server.Serve(lMain); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
