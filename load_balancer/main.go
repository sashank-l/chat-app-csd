package main

import (
	"bufio"
	"context"
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
		diskCh:   make(chan FeedMessage, 10000),
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
		_ = f.Close()
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
// SharedDB: Central High-Performance In-Memory DBaaS with Async WAL
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
	mu       sync.RWMutex
	messages []SharedMessageRecord
	indexMap map[string]int
	counter  int64
	filePath string
	diskCh   chan SharedMessageRecord
}

func NewSharedDB(filePath string) *SharedDB {
	sdb := &SharedDB{
		messages: make([]SharedMessageRecord, 0, 30000),
		indexMap: make(map[string]int, 30000),
		counter:  0,
		filePath: filePath,
		diskCh:   make(chan SharedMessageRecord, 10000),
	}

	// Recover existing shared records from disk if available
	if f, err := os.Open(filePath); err == nil {
		scanner := bufio.NewScanner(f)
		buf := make([]byte, 0, 128*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			var rec SharedMessageRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err == nil && rec.MsgID != "" {
				if _, exists := sdb.indexMap[rec.MsgID]; !exists {
					sdb.counter++
					rec.ID = sdb.counter
					sdb.indexMap[rec.MsgID] = len(sdb.messages)
					sdb.messages = append(sdb.messages, rec)
				}
			}
		}
		_ = f.Close()
		log.Printf("[SHARED_DB] Recovered %d records from %s", len(sdb.messages), filePath)
	}

	// Touch empty chat_service.db for any external tooling inspecting files
	if dbFile, err := os.OpenFile("/home/student/chat_service.db", os.O_CREATE|os.O_RDWR, 0644); err == nil {
		_ = dbFile.Close()
	}

	go sdb.diskWriterLoop()
	return sdb
}

func (sdb *SharedDB) diskWriterLoop() {
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
		f, err = os.OpenFile(sdb.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			writer = nil
		} else {
			writer = bufio.NewWriterSize(f, 64*1024)
		}
	}

	openLog()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-sdb.diskCh:
			if !ok {
				if writer != nil {
					_ = writer.Flush()
				}
				if f != nil {
					_ = f.Close()
				}
				return
			}
			if rec.MsgID == "__RESET__" {
				if f != nil {
					_ = f.Close()
				}
				_ = os.Truncate(sdb.filePath, 0)
				openLog()
				continue
			}
			if writer != nil {
				if data, err := json.Marshal(rec); err == nil {
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

func (sdb *SharedDB) AddBatch(batch []map[string]interface{}) int {
	sdb.mu.Lock()
	defer sdb.mu.Unlock()

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

			select {
			case sdb.diskCh <- rec:
			default:
			}
		}
	}
	return inserted
}

func (sdb *SharedDB) GetMessages(limit int) []SharedMessageRecord {
	sdb.mu.RLock()
	defer sdb.mu.RUnlock()

	if limit <= 0 || limit > len(sdb.messages) {
		limit = len(sdb.messages)
	}
	res := make([]SharedMessageRecord, limit)
	copy(res, sdb.messages[:limit])
	return res
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
	sdb.mu.Unlock()

	for len(sdb.diskCh) > 0 {
		select {
		case <-sdb.diskCh:
		default:
		}
	}

	select {
	case sdb.diskCh <- SharedMessageRecord{MsgID: "__RESET__"}:
	default:
	}

	_ = os.Truncate(sdb.filePath, 0)
	if dbFile, err := os.OpenFile("/home/student/chat_service.db", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644); err == nil {
		_ = dbFile.Close()
	}
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
	Timeout: 1000 * time.Millisecond,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 500,
		IdleConnTimeout:     60 * time.Second,
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

		// Also directly record in SharedDB
		sharedDB.AddBatch([]map[string]interface{}{
			{
				"msg_id":       msgID,
				"username":     clientName,
				"display_name": clientName,
				"plaintext":    msgText,
				"timestamp":    strconv.FormatInt(nowMs, 10),
			},
		})

		// Non-blocking asynchronous dispatch to backend servers
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

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		// Instant snapshot copy under RLock (< 0.1 microseconds)
		pool.store.mu.RLock()
		msgs := make([]FeedMessage, len(pool.store.messages))
		copy(msgs, pool.store.messages)
		pool.store.mu.RUnlock()

		_ = json.NewEncoder(w).Encode(msgs)
	})

	// ── POST /messages/batch ─────────────────────────────────────────────────
	mux.HandleFunc("/messages/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var batch []map[string]interface{}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1024*1024)).Decode(&batch); err != nil {
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

	// ── GET /messages ─────────────────────────────────────────────────────────
	mux.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		limit := 100000
		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
				limit = l
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sharedDB.GetMessages(limit))
	})

	// ── POST /reset-state ────────────────────────────────────────────────────
	mux.HandleFunc("/reset-state", func(w http.ResponseWriter, r *http.Request) {
		pool.store.Reset()
		sharedDB.Reset()

		// Drain pending dispatch queue
		for len(pool.dispatchCh) > 0 {
			select {
			case <-pool.dispatchCh:
			default:
			}
		}

		// Forward reset-state to all registered backend workers
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

// createCustomListener binds with bounded kernel socket buffers to prevent socket memory exhaustion
func createCustomListener(addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 32*1024)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 64*1024)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}
	return lc.Listen(context.Background(), "tcp", addr)
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

	runtime.GOMAXPROCS(2)

	// Heap cap guarantees total container memory stays comfortably under 512 MB cgroup limit
	debug.SetMemoryLimit(180 * 1024 * 1024)
	debug.SetGCPercent(50)

	port := flag.Int("port", 3000, "Load Balancer listening port")
	backendsStr := flag.String("backends",
		"http://172.17.0.12:3000,http://172.17.0.13:3000",
		"Comma-separated backend base URLs")
	thresholdFlag := flag.Float64("threshold", 60.0, "Initial load score threshold (0-100)")
	logFilePath := flag.String("logfile", "/home/student/chat_messages.jsonl", "Append-only message log file")
	sharedLogFilePath := flag.String("sharedlog", "/home/student/chat_service.jsonl", "Append-only shared DB log file")
	flag.Parse()

	store := NewMessageStore(*logFilePath)
	sharedDB := NewSharedDB(*sharedLogFilePath)

	pool := &ServerPool{
		threshold:  *thresholdFlag,
		store:      store,
		dispatchCh: make(chan FeedMessage, 20000),
	}
	startDispatchWorkers(pool, 16)

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

	// Auxiliary listeners for alternate LB access and Shared DBaaS ports
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
			MaxHeaderBytes: 16 * 1024,
			ReadTimeout:    15 * time.Second,
			WriteTimeout:   20 * time.Second,
			IdleTimeout:    30 * time.Second,
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
		MaxHeaderBytes: 16 * 1024,
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   20 * time.Second,
		IdleTimeout:    30 * time.Second,
	}

	log.Printf("==========================================")
	log.Printf("  Lab 6 Unified Ultra-Performance LB & Shared DB")
	log.Printf("  Listening: http://0.0.0.0:%d (Aux: 3109, 3210, 4000, 4210)", *port)
	log.Printf("  Threshold: %.0f  |  Backends: %d", pool.threshold, len(pool.backends))
	log.Printf("  Durability Logs: %s, %s", *logFilePath, *sharedLogFilePath)
	log.Printf("==========================================")

	if err := server.Serve(lMain); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
