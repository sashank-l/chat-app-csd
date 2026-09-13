package main

import (
	"bufio"
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
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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

type FeedMessage struct {
	ID         string `json:"id"`
	ClientName string `json:"client-name"`
	Msg        string `json:"msg"`
	Timestamp  int64  `json:"timestamp"`
}

type MessageStore struct {
	mu       sync.RWMutex
	messages []FeedMessage
	seen     map[string]bool
	fileMu   sync.Mutex
	logFile  *os.File
	filePath string
}

func NewMessageStore(filePath string) *MessageStore {
	ms := &MessageStore{
		messages: make([]FeedMessage, 0, 100000),
		seen:     make(map[string]bool, 100000),
		filePath: filePath,
	}

	if f, err := os.Open(filePath); err == nil {
		scanner := bufio.NewScanner(f)
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

	logF, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("[STORE] Warning: could not open log file %s: %v", filePath, err)
	} else {
		ms.logFile = logF
	}

	return ms
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
	ms.mu.Unlock()

	go func(m FeedMessage) {
		ms.fileMu.Lock()
		defer ms.fileMu.Unlock()
		if ms.logFile != nil {
			if data, err := json.Marshal(m); err == nil {
				ms.logFile.Write(append(data, '\n'))
			}
		}
	}(msg)

	return true
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
	ms.messages = make([]FeedMessage, 0, 100000)
	ms.seen = make(map[string]bool, 100000)
	ms.mu.Unlock()

	ms.fileMu.Lock()
	if ms.logFile != nil {
		ms.logFile.Close()
	}
	_ = os.Truncate(ms.filePath, 0)
	if f, err := os.OpenFile(ms.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		ms.logFile = f
	}
	ms.fileMu.Unlock()
	log.Printf("[STORE] Message store reset complete")
}

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
	backends  []*Backend
	threshold float64
	mu        sync.RWMutex
	store     *MessageStore
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
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
	},
}

var feedHttpClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
	},
}

type ReplicationTask struct {
	MsgID      string
	ClientName string
	MsgText    string
}

var replicationCh = make(chan ReplicationTask, 100000)

func startReplicationWorkers(pool *ServerPool, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go func() {
			for task := range replicationCh {
				target := pool.SelectBest()
				if target == nil {
					continue
				}

				payload := url.Values{
					"client-name": {task.ClientName},
					"msg":         {task.MsgText},
					"msg_id":      {task.MsgID},
				}

				atomic.AddInt64(&target.ActiveInFlight, 1)
				resp, err := httpClient.PostForm(target.URL+"/message", payload)
				atomic.AddInt64(&target.ActiveInFlight, -1)

				if err == nil && resp != nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode < 400 {
						target.RecordSuccess()
						atomic.AddInt64(&target.TotalServed, 1)
					} else {
						target.RecordFailure()
						atomic.AddInt64(&target.TotalErrors, 1)
					}
				} else {
					if resp != nil {
						resp.Body.Close()
					}
					target.RecordFailure()
					atomic.AddInt64(&target.TotalErrors, 1)
				}
			}
		}()
	}
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

var (
	totalRequests int64
	totalErrors   int64
)

func makeHandler(pool *ServerPool) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)
		t0 := time.Now()

		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var clientName, msgText, msgID string

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}

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
			if v, ok := jsonBody["msg_id"]; ok {
				msgID = fmt.Sprint(v)
			} else if v, ok := jsonBody["id"]; ok {
				msgID = fmt.Sprint(v)
			}
		}

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
				if msgID == "" {
					msgID = vals.Get("msg_id")
					if msgID == "" {
						msgID = vals.Get("id")
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
			msgID = newUUID()
		}

		nowMs := time.Now().UnixMilli()
		feedMsg := FeedMessage{
			ID:         msgID,
			ClientName: clientName,
			Msg:        msgText,
			Timestamp:  nowMs,
		}

		pool.store.Add(feedMsg)

		select {
		case replicationCh <- ReplicationTask{MsgID: msgID, ClientName: clientName, MsgText: msgText}:
		default:
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		respBytes, _ := json.Marshal(map[string]interface{}{
			"status":      "ok",
			"msg_id":      msgID,
			"client-name": clientName,
			"msg":         msgText,
			"timestamp":   nowMs,
			"latency_ms":  time.Since(t0).Milliseconds(),
		})
		w.Write(respBytes)
	})

	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&totalRequests, 1)

		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		allMsgs := pool.store.GetAll()

		if len(allMsgs) == 0 {
			backends := pool.GetAll()
			type feedResult struct {
				msgs []FeedMessage
			}
			ch := make(chan feedResult, len(backends))
			for _, b := range backends {
				go func(backendURL string) {
					resp, err := feedHttpClient.Get(backendURL + "/feed")
					if err != nil || resp.StatusCode != http.StatusOK {
						if resp != nil {
							resp.Body.Close()
						}
						ch <- feedResult{nil}
						return
					}
					defer resp.Body.Close()
					var msgs []FeedMessage
					json.NewDecoder(resp.Body).Decode(&msgs)
					ch <- feedResult{msgs}
				}(b.URL)
			}

			seen := make(map[string]bool)
			for range backends {
				res := <-ch
				for _, m := range res.msgs {
					key := m.ID
					if key == "" {
						key = fmt.Sprintf("%s_%s_%d", m.ClientName, m.Msg, m.Timestamp)
					}
					if !seen[key] {
						seen[key] = true
						allMsgs = append(allMsgs, m)
						pool.store.Add(m)
					}
				}
			}
		}

		sort.Slice(allMsgs, func(i, j int) bool {
			return allMsgs[i].Timestamp < allMsgs[j].Timestamp
		})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(allMsgs)
	})

	mux.HandleFunc("/reset-state", func(w http.ResponseWriter, r *http.Request) {
		pool.store.Reset()

		for _, b := range pool.GetAll() {
			go func(url string) {
				req, _ := http.NewRequest(http.MethodPost, url+"/reset-state", nil)
				resp, err := httpClient.Do(req)
				if err == nil && resp != nil {
					resp.Body.Close()
				}
			}(b.URL)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"message": "load balancer state reset complete",
		})
	})

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
			"stored_msgs":    pool.store.Count(),
			"total_requests": atomic.LoadInt64(&totalRequests),
			"total_errors":   atomic.LoadInt64(&totalErrors),
			"backends":       stats,
		})
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "ok",
			"role":        "load-balancer",
			"stored_msgs": pool.store.Count(),
		})
	})

	return mux
}

func main() {
	port := flag.Int("port", 3000, "Load Balancer listening port")
	backendsStr := flag.String("backends",
		"http://172.17.0.11:4000,http://172.17.0.12:3000,http://172.17.0.13:3000",
		"Comma-separated backend base URLs")
	thresholdFlag := flag.Float64("threshold", 60.0, "Initial load score threshold (0-100)")
	logFilePath := flag.String("logfile", "/home/student/chat_messages.jsonl", "Append-only message log file")
	flag.Parse()

	store := NewMessageStore(*logFilePath)

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

	startReplicationWorkers(pool, 32)
	go healthCheck(pool, 2*time.Second)

	handler := makeHandler(pool)

	go func() {
		sDual := &http.Server{
			Addr:         "0.0.0.0:3210",
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 20 * time.Second,
			IdleTimeout:  60 * time.Second,
		}
		log.Printf("[DUAL] Also listening on http://0.0.0.0:3210")
		if err := sDual.ListenAndServe(); err != nil {
			log.Printf("[DUAL] Port 3210 listener: %v", err)
		}
	}()

	go func() {
		s3109 := &http.Server{
			Addr:         "0.0.0.0:3109",
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 20 * time.Second,
			IdleTimeout:  60 * time.Second,
		}
		log.Printf("[PORT] Also listening on http://0.0.0.0:3109")
		if err := s3109.ListenAndServe(); err != nil {
			log.Printf("[PORT] Port 3109 listener: %v", err)
		}
	}()

	server := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", *port),
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("==========================================")
	log.Printf("  Lab 6 Ultra-Performance Dynamic Load Balancer")
	log.Printf("  Listening: http://0.0.0.0:%d, 3109, 3210", *port)
	log.Printf("  Threshold: %.0f  |  Backends: %d", pool.threshold, len(pool.backends))
	log.Printf("  Durability Log: %s", *logFilePath)
	log.Printf("==========================================")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
