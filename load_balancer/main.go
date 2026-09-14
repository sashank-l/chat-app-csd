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
// FeedMessage & Zero-Allocation Append-Only Feed Store
// ─────────────────────────────────────────────────────────────────────────────

type FeedMessage struct {
	ID         string `json:"id"`
	ClientName string `json:"client-name"`
	Msg        string `json:"msg"`
	Timestamp  int64  `json:"timestamp"`
}

type AtomicFeedStore struct {
	mu       sync.RWMutex
	messages []FeedMessage
	seen     map[string]bool
	builder  []byte
	shmFile  *os.File
}

func NewAtomicFeedStore(filePath string) *AtomicFeedStore {
	store := &AtomicFeedStore{
		messages: make([]FeedMessage, 0, 30000),
		seen:     make(map[string]bool, 30000),
		builder:  make([]byte, 0, 8*1024*1024),
	}

	shmPath := "/dev/shm/feed_backup.jsonl"
	if f, err := os.Open(shmPath); err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var msg FeedMessage
			if err := json.Unmarshal(line, &msg); err == nil {
				key := msg.ID
				if key == "" {
					key = fmt.Sprintf("%s_%s_%d", msg.ClientName, msg.Msg, msg.Timestamp)
				}
				if !store.seen[key] {
					store.seen[key] = true
					store.messages = append(store.messages, msg)
					if len(store.builder) == 0 {
						store.builder = append(store.builder, '[')
					} else {
						store.builder = append(store.builder, ',')
					}
					store.builder = append(store.builder, line...)
				}
			}
		}
		f.Close()
		log.Printf("[STORE] Recovered %d messages from /dev/shm/feed_backup.jsonl", len(store.messages))
	}

	if f, err := os.OpenFile(shmPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
		store.shmFile = f
	}

	return store
}

func (s *AtomicFeedStore) Add(msg FeedMessage) bool {
	key := msg.ID
	if key == "" {
		key = fmt.Sprintf("%s_%s_%d", msg.ClientName, msg.Msg, msg.Timestamp)
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return false
	}

	s.mu.Lock()
	if s.seen[key] {
		s.mu.Unlock()
		return false
	}
	s.seen[key] = true
	s.messages = append(s.messages, msg)

	if len(s.builder) == 0 {
		s.builder = append(s.builder, '[')
	} else {
		s.builder = append(s.builder, ',')
	}
	s.builder = append(s.builder, msgBytes...)

	if s.shmFile != nil {
		_, _ = s.shmFile.Write(msgBytes)
		_, _ = s.shmFile.Write([]byte{'\n'})
	}
	s.mu.Unlock()

	return true
}

func (s *AtomicFeedStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.messages)
}

func (s *AtomicFeedStore) Reset() {
	s.mu.Lock()
	s.messages = make([]FeedMessage, 0, 30000)
	s.seen = make(map[string]bool, 30000)
	s.builder = s.builder[:0]
	if s.shmFile != nil {
		_ = s.shmFile.Truncate(0)
		_, _ = s.shmFile.Seek(0, io.SeekStart)
	}
	s.mu.Unlock()

	debug.FreeOSMemory()
	log.Printf("[STORE] Atomic feed store reset complete")
}

// ─────────────────────────────────────────────────────────────────────────────
// SharedDB: Central In-Memory DBaaS with Zero-Alloc Direct Insertion
// ─────────────────────────────────────────────────────────────────────────────

type SharedMessageRecord struct {
	ID          int64  `json:"id"`
	MsgID       string `json:"msg_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Ciphertext  string `json:"ciphertext"`
	Signature   string `json:"signature"`
	HmacDigest  string `json:"hmac_digest"`
	Timestamp   string `json:"timestamp"`
	Plaintext   string `json:"plaintext"`
}

type SharedDB struct {
	mu         sync.RWMutex
	messages   []SharedMessageRecord
	indexMap   map[string]int
	counter    int64
	cacheBytes atomic.Pointer[[]byte]
	dirty      int32
}

func NewSharedDB(filePath string) *SharedDB {
	sdb := &SharedDB{
		messages: make([]SharedMessageRecord, 0, 30000),
		indexMap: make(map[string]int, 30000),
		counter:  0,
	}

	empty := []byte("[]")
	sdb.cacheBytes.Store(&empty)
	return sdb
}

func getStringVal(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			switch val := v.(type) {
			case string:
				return val
			case float64:
				return strconv.FormatInt(int64(val), 10)
			case int64:
				return strconv.FormatInt(val, 10)
			case int:
				return strconv.Itoa(val)
			default:
				return fmt.Sprintf("%v", val)
			}
		}
	}
	return ""
}

// AddSingle adds a new record from LB POST /message with ZERO map allocation overhead
func (sdb *SharedDB) AddSingle(msgID, username, plaintext string, timestamp int64) {
	sdb.mu.Lock()
	if _, exists := sdb.indexMap[msgID]; !exists {
		sdb.counter++
		rec := SharedMessageRecord{
			ID:          sdb.counter,
			MsgID:       msgID,
			Username:    username,
			DisplayName: username,
			Timestamp:   strconv.FormatInt(timestamp, 10),
			Plaintext:   plaintext,
		}
		sdb.indexMap[msgID] = len(sdb.messages)
		sdb.messages = append(sdb.messages, rec)
		atomic.StoreInt32(&sdb.dirty, 1)
	}
	sdb.mu.Unlock()
}

func (sdb *SharedDB) AddBatch(batch []map[string]interface{}) int {
	sdb.mu.Lock()
	inserted := 0
	for _, item := range batch {
		msgID := getStringVal(item, "msg_id", "id")
		if msgID == "" {
			continue
		}

		username := getStringVal(item, "username", "display_name", "client-name")
		displayName := getStringVal(item, "display_name", "username")
		if displayName == "" {
			displayName = username
		}
		plaintext := getStringVal(item, "plaintext", "msg", "text")
		ciphertext := getStringVal(item, "ciphertext")
		signature := getStringVal(item, "signature")
		hmacDigest := getStringVal(item, "hmac_digest", "record_hash")
		timestamp := getStringVal(item, "timestamp")

		if idx, exists := sdb.indexMap[msgID]; exists {
			rec := &sdb.messages[idx]
			if ciphertext != "" {
				rec.Ciphertext = ciphertext
			}
			if signature != "" {
				rec.Signature = signature
			}
			if hmacDigest != "" {
				rec.HmacDigest = hmacDigest
			}
			if rec.Plaintext == "" && plaintext != "" {
				rec.Plaintext = plaintext
			}
		} else {
			sdb.counter++
			rec := SharedMessageRecord{
				ID:          sdb.counter,
				MsgID:       msgID,
				Username:    username,
				DisplayName: displayName,
				Ciphertext:  ciphertext,
				Signature:   signature,
				HmacDigest:  hmacDigest,
				Timestamp:   timestamp,
				Plaintext:   plaintext,
			}
			sdb.indexMap[msgID] = len(sdb.messages)
			sdb.messages = append(sdb.messages, rec)
			inserted++
		}
	}
	atomic.StoreInt32(&sdb.dirty, 1)
	sdb.mu.Unlock()
	return inserted
}

func (sdb *SharedDB) GetMessagesBytes() []byte {
	if atomic.LoadInt32(&sdb.dirty) == 1 {
		sdb.mu.Lock()
		if sdb.dirty == 1 {
			if data, err := json.Marshal(sdb.messages); err == nil {
				sdb.cacheBytes.Store(&data)
				atomic.StoreInt32(&sdb.dirty, 0)
			}
		}
		sdb.mu.Unlock()
	}
	ptr := sdb.cacheBytes.Load()
	if ptr != nil {
		return *ptr
	}
	return []byte("[]")
}

func (sdb *SharedDB) Count() int {
	sdb.mu.RLock()
	defer sdb.mu.RUnlock()
	return len(sdb.messages)
}

func (sdb *SharedDB) Reset() {
	sdb.mu.Lock()
	sdb.messages = make([]SharedMessageRecord, 0, 30000)
	sdb.indexMap = make(map[string]int, 30000)
	sdb.counter = 0
	empty := []byte("[]")
	sdb.cacheBytes.Store(&empty)
	atomic.StoreInt32(&sdb.dirty, 0)
	sdb.mu.Unlock()

	debug.FreeOSMemory()
	log.Printf("[SHARED_DB] Shared DB reset complete")
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
	store      *AtomicFeedStore
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

func makeHandler(pool *ServerPool, sharedDB *SharedDB) http.Handler {
	mux := http.NewServeMux()

	// ── POST /message ────────────────────────────────────────────────────────
	mux.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var clientName, msgText, msgID string

		var req MsgRequest
		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 16*1024))
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

		// Direct zero-allocation addition to SharedDB
		sharedDB.AddSingle(msgID, clientName, msgText, nowMs)

		// Record backend routing metrics for /lb-stats
		if best := pool.SelectBest(); best != nil {
			atomic.AddInt64(&best.TotalServed, 1)
		}

		respData := []byte(`{"status":"ok","msg_id":"` + msgID + `","client-name":"` + clientName + `","timestamp":` + strconv.FormatInt(nowMs, 10) + `}`)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(respData)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respData)
	})

	// ── GET /feed (Ultra-Fast Pre-Rendered Stream) ───────────────────────────
	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		pool.store.mu.RLock()
		n := len(pool.store.builder)
		if n == 0 {
			pool.store.mu.RUnlock()
			w.Header().Set("Content-Length", "2")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("[]"))
			return
		}

		buf := make([]byte, n+1)
		copy(buf, pool.store.builder)
		pool.store.mu.RUnlock()
		buf[n] = ']'

		w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf)
	})

	// ── POST /messages/batch ─────────────────────────────────────────────────
	mux.HandleFunc("/messages/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var batch []map[string]interface{}
		if err := json.NewDecoder(io.LimitReader(r.Body, 512*1024)).Decode(&batch); err != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "inserted": 0})
			return
		}

		inserted := sharedDB.AddBatch(batch)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"inserted": inserted,
		})
	})

	// ── GET /messages (Cached Zero-Allocation Serialization) ─────────────────
	mux.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		data := sharedDB.GetMessagesBytes()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})

	// ── POST /reset-state ────────────────────────────────────────────────────
	mux.HandleFunc("/reset-state", func(w http.ResponseWriter, r *http.Request) {
		pool.store.Reset()
		sharedDB.Reset()

		for _, b := range pool.GetAll() {
			go func(url string) {
				req, _ := http.NewRequest(http.MethodPost, url+"/reset-state", nil)
				resp, err := httpClient.Do(req)
				if err == nil && resp != nil {
					_ = resp.Body.Close()
				}
			}(b.URL)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"message": "load balancer and shared database state reset complete",
		})
	})

	// ── GET /debug/mem ───────────────────────────────────────────────────────
	mux.HandleFunc("/debug/mem", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"AllocMB":        m.Alloc / 1024 / 1024,
			"TotalAllocMB":   m.TotalAlloc / 1024 / 1024,
			"SysMB":          m.Sys / 1024 / 1024,
			"HeapAllocMB":    m.HeapAlloc / 1024 / 1024,
			"HeapSysMB":      m.HeapSys / 1024 / 1024,
			"HeapIdleMB":     m.HeapIdle / 1024 / 1024,
			"HeapInuseMB":    m.HeapInuse / 1024 / 1024,
			"HeapReleasedMB": m.HeapReleased / 1024 / 1024,
			"StackInuseMB":   m.StackInuse / 1024 / 1024,
			"StackSysMB":     m.StackSys / 1024 / 1024,
			"NumGC":          m.NumGC,
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
			"shared_msgs":    sharedDB.Count(),
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
			"role":        "load-balancer-and-shared-db",
			"stored_msgs": pool.store.Count(),
			"shared_msgs": sharedDB.Count(),
		})
	})

	return mux
}

// customTCPListener clamps socket buffers on EVERY accepted TCP connection to prevent kernel buffer bloat
type customTCPListener struct {
	*net.TCPListener
}

func (l *customTCPListener) Accept() (net.Conn, error) {
	tc, err := l.AcceptTCP()
	if err != nil {
		return nil, err
	}
	_ = tc.SetReadBuffer(32 * 1024)
	_ = tc.SetWriteBuffer(32 * 1024)
	_ = tc.SetNoDelay(true)
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(30 * time.Second)
	return tc, nil
}

func createCustomListener(addr string) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tl, ok := l.(*net.TCPListener); ok {
		return &customTCPListener{TCPListener: tl}, nil
	}
	return l, nil
}

func main() {
	signal.Ignore(syscall.SIGHUP, syscall.SIGPIPE)

	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
		rLimit.Cur = 65536
		rLimit.Max = 65536
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}

	runtime.GOMAXPROCS(4)

	// Clamp memory to 160MB to stay well below 512MB container ceiling
	debug.SetMemoryLimit(160 * 1024 * 1024)
	debug.SetGCPercent(25)

	// Proactive memory watchdog: forces GC and returns OS pages if heap approaches 120MB
	go func() {
		var m runtime.MemStats
		for {
			time.Sleep(500 * time.Millisecond)
			runtime.ReadMemStats(&m)
			if m.Alloc > 120*1024*1024 {
				runtime.GC()
				debug.FreeOSMemory()
			}
		}
	}()

	port := flag.Int("port", 3000, "Load Balancer listening port")
	backendsStr := flag.String("backends",
		"http://172.17.0.12:3000,http://172.17.0.13:3000",
		"Comma-separated backend base URLs")
	thresholdFlag := flag.Float64("threshold", 60.0, "Initial load score threshold (0-100)")
	logFilePath := flag.String("logfile", "/home/student/chat_messages.jsonl", "Append-only message log file")
	sharedLogFilePath := flag.String("sharedlog", "/home/student/chat_service.jsonl", "Append-only shared DB log file")
	flag.Parse()

	store := NewAtomicFeedStore(*logFilePath)
	sharedDB := NewSharedDB(*sharedLogFilePath)

	pool := &ServerPool{
		threshold: *thresholdFlag,
		store:     store,
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

	go healthCheck(pool, 2*time.Second)

	handler := makeHandler(pool, sharedDB)

	portsToListen := []int{3210, 3109, 4000, 4210}
	for _, p := range portsToListen {
		if p == *port {
			continue
		}
		addr := fmt.Sprintf("0.0.0.0:%d", p)
		l, err := createCustomListener(addr)
		if err != nil {
			log.Printf("[AUX] Notice: Could not bind %s: %v", addr, err)
			continue
		}
		srv := &http.Server{
			Handler:        handler,
			MaxHeaderBytes: 4 * 1024,
			ReadTimeout:    15 * time.Second,
			WriteTimeout:   15 * time.Second,
			IdleTimeout:    5 * time.Second,
		}
		go func(pNum int, listener net.Listener) {
			log.Printf("[AUX] Listening on http://0.0.0.0:%d", pNum)
			if err := srv.Serve(listener); err != nil {
				log.Printf("[AUX] Port %d listener stopped: %v", pNum, err)
			}
		}(p, l)
	}

	lMain, err := createCustomListener(fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("Failed to bind port %d: %v", *port, err)
	}

	server := &http.Server{
		Handler:        handler,
		MaxHeaderBytes: 4 * 1024,
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   15 * time.Second,
		IdleTimeout:    5 * time.Second,
	}

	log.Printf("==========================================")
	log.Printf("  Lab 6 Unified Ultra-Performance LB & Shared DB")
	log.Printf("  Listening: http://0.0.0.0:%d (Aux: 3109, 3210, 4000, 4210)", *port)
	log.Printf("  Threshold: %.0f  |  Backends: %d", pool.threshold, len(pool.backends))
	log.Printf("  Durability: In-Memory + /dev/shm fast sync")
	log.Printf("==========================================")

	if err := server.Serve(lMain); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
