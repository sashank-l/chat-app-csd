package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
	},
}

func randString(minLen, maxLen int) string {
	length := minLen + rand.Intn(maxLen-minLen+1)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,!?-"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randClientName() string {
	names := []string{"alice", "bob", "charlie", "diana", "eve", "frank", "grace", "hank", "ivy", "jack"}
	return names[rand.Intn(len(names))] + fmt.Sprintf("_%03d", rand.Intn(1000))
}

func percentile(latencies []float64, p float64) float64 {
	if len(latencies) == 0 {
		return 0
	}
	sort.Float64s(latencies)
	idx := int(math.Ceil((p/100.0)*float64(len(latencies)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(latencies) {
		idx = len(latencies) - 1
	}
	return latencies[idx]
}

func main() {
	targetURL := flag.String("url", "http://10.1.75.51:3210", "Load Balancer base URL")
	numClients := flag.Int("clients", 50, "Number of concurrent virtual users")
	durationSec := flag.Int("duration", 30, "Test duration in seconds")
	minMsgLen := flag.Int("min-len", 10, "Minimum message length in chars")
	maxMsgLen := flag.Int("max-len", 300, "Maximum message length in chars")
	minIntervalMs := flag.Int("min-interval", 100, "Minimum interval between messages (ms)")
	maxIntervalMs := flag.Int("max-interval", 500, "Maximum interval between messages (ms)")
	feedEvery := flag.Int("feed-every", 5, "How often (seconds) to call GET /feed per client")
	flag.Parse()

	base := strings.TrimRight(*targetURL, "/")
	messageURL := base + "/message"
	feedURL := base + "/feed"

	fmt.Printf("=============================================================\n")
	fmt.Printf("  Lab 6 Load Generator\n")
	fmt.Printf("  Target:   %s\n", base)
	fmt.Printf("  Clients:  %d  |  Duration: %ds\n", *numClients, *durationSec)
	fmt.Printf("  Msg Len:  %d–%d chars  |  Interval: %d–%dms\n", *minMsgLen, *maxMsgLen, *minIntervalMs, *maxIntervalMs)
	fmt.Printf("=============================================================\n")

	var (
		msgSent     int64
		msgSuccess  int64
		msgError    int64
		feedSuccess int64
		feedError   int64

		latencies []float64
		latMu     sync.Mutex
		wg        sync.WaitGroup
		stopChan  = make(chan struct{})
	)

	startTime := time.Now()

	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		clientName := randClientName()

		go func(cname string) {
			defer wg.Done()

			lastFeed := time.Now()

			for {
				select {
				case <-stopChan:
					return
				default:
				}

				// Random interval (Poisson-like jitter)
				interval := *minIntervalMs + rand.Intn(*maxIntervalMs-*minIntervalMs+1)

				// Send a message
				msgText := randString(*minMsgLen, *maxMsgLen)
				t0 := time.Now()
				resp, err := httpClient.PostForm(messageURL, url.Values{
					"client-name": {cname},
					"msg":         {msgText},
				})
				latMs := float64(time.Since(t0).Microseconds()) / 1000.0

				atomic.AddInt64(&msgSent, 1)
				if err != nil || resp.StatusCode >= 400 {
					atomic.AddInt64(&msgError, 1)
					if resp != nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				} else {
					atomic.AddInt64(&msgSuccess, 1)
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()

					latMu.Lock()
					latencies = append(latencies, latMs)
					latMu.Unlock()
				}

				// Periodically call /feed
				if time.Since(lastFeed) >= time.Duration(*feedEvery)*time.Second {
					lastFeed = time.Now()
					t1 := time.Now()
					fresp, ferr := httpClient.Get(feedURL)
					_ = t1
					if ferr != nil || fresp.StatusCode >= 400 {
						atomic.AddInt64(&feedError, 1)
						if fresp != nil {
							fresp.Body.Close()
						}
					} else {
						var msgs []interface{}
						json.NewDecoder(fresp.Body).Decode(&msgs)
						fresp.Body.Close()
						atomic.AddInt64(&feedSuccess, 1)
					}
				}

				// Wait before next message
				select {
				case <-stopChan:
					return
				case <-time.After(time.Duration(interval) * time.Millisecond):
				}
			}
		}(clientName)

		// Stagger startup by 5ms per client
		time.Sleep(5 * time.Millisecond)
	}

	// Run for duration
	time.Sleep(time.Duration(*durationSec) * time.Second)
	close(stopChan)
	wg.Wait()

	totalTime := time.Since(startTime).Seconds()

	// Final /feed to measure persistence
	fmt.Printf("\n[*] Calling GET /feed to verify persistence...\n")
	feedResp, feedErr := httpClient.Get(feedURL)
	feedCount := 0
	if feedErr == nil {
		var msgs []interface{}
		json.NewDecoder(feedResp.Body).Decode(&msgs)
		feedResp.Body.Close()
		feedCount = len(msgs)
	}

	// Compute latency stats
	latMu.Lock()
	lats := append([]float64(nil), latencies...)
	latMu.Unlock()

	var sumLat, minLat, maxLat float64
	if len(lats) > 0 {
		minLat = lats[0]
		for _, l := range lats {
			sumLat += l
			if l < minLat {
				minLat = l
			}
			if l > maxLat {
				maxLat = l
			}
		}
	}
	avgLat := 0.0
	if len(lats) > 0 {
		avgLat = sumLat / float64(len(lats))
	}

	p50 := percentile(lats, 50)
	p95 := percentile(lats, 95)
	p99 := percentile(lats, 99)
	throughput := float64(msgSuccess) / totalTime
	errorRate := 0.0
	if msgSent > 0 {
		errorRate = float64(msgError) / float64(msgSent) * 100.0
	}
	persistenceRate := 0.0
	if msgSuccess > 0 {
		persistenceRate = float64(feedCount) / float64(msgSuccess) * 100.0
	}

	fmt.Printf("\n===================== BENCHMARK RESULTS =====================\n")
	fmt.Printf("  Target URL:           %s\n", base)
	fmt.Printf("  Concurrent Clients:   %d\n", *numClients)
	fmt.Printf("  Total Duration:       %.2f s\n", totalTime)
	fmt.Printf("\n  -- Message Stats --\n")
	fmt.Printf("  Messages Sent:        %d\n", msgSent)
	fmt.Printf("  Accepted (2xx):       %d\n", msgSuccess)
	fmt.Printf("  Errors:               %d  (%.1f%%)\n", msgError, errorRate)
	fmt.Printf("  Throughput:           %.2f msg/sec\n", throughput)
	fmt.Printf("\n  -- Persistence Check --\n")
	fmt.Printf("  Messages in /feed:    %d\n", feedCount)
	fmt.Printf("  Persistence Rate:     %.1f%%\n", persistenceRate)
	fmt.Printf("\n  -- Latency Distribution (POST /message) --\n")
	fmt.Printf("  Min:                  %.2f ms\n", minLat)
	fmt.Printf("  Avg:                  %.2f ms\n", avgLat)
	fmt.Printf("  p50 (Median):         %.2f ms\n", p50)
	fmt.Printf("  p95:                  %.2f ms\n", p95)
	fmt.Printf("  p99:                  %.2f ms\n", p99)
	fmt.Printf("  Max:                  %.2f ms\n", maxLat)
	fmt.Printf("\n  -- Feed Stats --\n")
	fmt.Printf("  Feed Success:         %d\n", feedSuccess)
	fmt.Printf("  Feed Errors:          %d\n", feedError)
	fmt.Printf("=============================================================\n")
}
