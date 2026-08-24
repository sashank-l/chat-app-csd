package main

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Simple self-contained RFC-6455 WebSocket client using only Go standard library.
type WSConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func DialWS(targetURL string) (*WSConn, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return nil, err
	}

	// Generate Sec-WebSocket-Key
	keyBytes := make([]byte, 16)
	rand.Read(keyBytes)
	secKey := base64.StdEncoding.EncodeToString(keyBytes)

	path := u.Path
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}

	req := fmt.Sprintf("GET %s HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\n"+
		"Sec-WebSocket-Version: 13\r\n\r\n", path, u.Host, secKey)

	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}

	reader := bufio.NewReader(conn)
	// Read response status line
	statusLine, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(statusLine, "101") {
		conn.Close()
		return nil, fmt.Errorf("handshake failed: %s (err: %v)", strings.TrimSpace(statusLine), err)
	}

	// Read remaining headers until empty line
	for {
		line, err := reader.ReadString('\n')
		if err != nil || strings.TrimSpace(line) == "" {
			break
		}
	}

	return &WSConn{conn: conn, r: reader}, nil
}

func (ws *WSConn) SendTextMessage(text string) error {
	payload := []byte(text)
	length := len(payload)

	var header []byte
	header = append(header, 0x81) // FIN + Text frame (0x1)

	maskKey := make([]byte, 4)
	rand.Read(maskKey)

	if length <= 125 {
		header = append(header, 0x80|byte(length)) // Mask bit set
	} else if length <= 65535 {
		header = append(header, 0x80|126)
		lenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBytes, uint16(length))
		header = append(header, lenBytes...)
	} else {
		header = append(header, 0x80|127)
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(length))
		header = append(header, lenBytes...)
	}

	header = append(header, maskKey...)

	maskedPayload := make([]byte, length)
	for i := 0; i < length; i++ {
		maskedPayload[i] = payload[i] ^ maskKey[i%4]
	}

	packet := append(header, maskedPayload...)
	_, err := ws.conn.Write(packet)
	return err
}

func (ws *WSConn) ReadMessage() (string, error) {
	b1, err := ws.r.ReadByte()
	if err != nil {
		return "", err
	}
	b2, err := ws.r.ReadByte()
	if err != nil {
		return "", err
	}

	isMasked := (b2 & 0x80) != 0
	payloadLen := int(b2 & 0x7F)

	if payloadLen == 126 {
		lenBytes := make([]byte, 2)
		if _, err := io.ReadFull(ws.r, lenBytes); err != nil {
			return "", err
		}
		payloadLen = int(binary.BigEndian.Uint16(lenBytes))
	} else if payloadLen == 127 {
		lenBytes := make([]byte, 8)
		if _, err := io.ReadFull(ws.r, lenBytes); err != nil {
			return "", err
		}
		payloadLen = int(binary.BigEndian.Uint64(lenBytes))
	}

	var maskKey []byte
	if isMasked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(ws.r, maskKey); err != nil {
			return "", err
		}
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(ws.r, payload); err != nil {
		return "", err
	}

	if isMasked {
		for i := 0; i < payloadLen; i++ {
			payload[i] ^= maskKey[i%4]
		}
	}

	opcode := b1 & 0x0F
	if opcode == 0x8 { // Connection close
		return "", io.EOF
	}

	return string(payload), nil
}

func (ws *WSConn) Close() {
	ws.conn.Close()
}

// Compute percentile
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
	targetURL := flag.String("url", "ws://172.17.0.10:4209/ws", "Target WebSocket URL (Load Balancer)")
	numClients := flag.Int("clients", 50, "Number of concurrent virtual clients")
	durationSec := flag.Int("duration", 20, "Test duration in seconds")
	msgIntervalMs := flag.Int("interval", 200, "Message sending interval per client in milliseconds")
	flag.Parse()

	log.Printf("==========================================================")
	log.Printf("🔥 Starting Load Generator against: %s", *targetURL)
	log.Printf("👥 Concurrent Clients: %d | ⏱ Duration: %ds | 📨 Interval: %dms", *numClients, *durationSec, *msgIntervalMs)
	log.Printf("==========================================================")

	var (
		totalSent     int64
		totalReceived int64
		totalErrors   int64
		latencies     []float64
		latMux        sync.Mutex
		wg            sync.WaitGroup
		stopChan      = make(chan struct{})
	)

	startTime := time.Now()

	// Launch concurrent virtual users
	for i := 1; i <= *numClients; i++ {
		wg.Add(1)
		clientID := fmt.Sprintf("bench_user_%03d", i)

		go func(id string, clientNum int) {
			defer wg.Done()

			ws, err := DialWS(*targetURL)
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				if clientNum <= 3 {
					log.Printf("❌ [%s] Connect error: %v", id, err)
				}
				return
			}
			defer ws.Close()

			// Send Join message
			joinMsg, _ := json.Marshal(map[string]interface{}{
				"type":     "join",
				"username": id,
			})
			if err := ws.SendTextMessage(string(joinMsg)); err != nil {
				atomic.AddInt64(&totalErrors, 1)
				return
			}

			// Reader Goroutine
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				for {
					_, err := ws.ReadMessage()
					if err != nil {
						return
					}
					atomic.AddInt64(&totalReceived, 1)
				}
			}()

			// Sender loop with latency measurement
			ticker := time.NewTicker(time.Duration(*msgIntervalMs) * time.Millisecond)
			defer ticker.Stop()

			msgCount := 0
			for {
				select {
				case <-stopChan:
					return
				case <-ticker.C:
					msgCount++
					sendTime := time.Now()
					msgPayload, _ := json.Marshal(map[string]interface{}{
						"type":      "message",
						"text":      fmt.Sprintf("Benchmark message %d from %s", msgCount, id),
						"timestamp": sendTime.UnixMilli(),
					})

					if err := ws.SendTextMessage(string(msgPayload)); err != nil {
						atomic.AddInt64(&totalErrors, 1)
						return
					}

					atomic.AddInt64(&totalSent, 1)
					elapsedMs := float64(time.Since(sendTime).Microseconds()) / 1000.0

					latMux.Lock()
					latencies = append(latencies, elapsedMs)
					latMux.Unlock()
				}
			}
		}(clientID, i)

		// Stagger connection startup slightly
		time.Sleep(10 * time.Millisecond)
	}

	// Run for duration
	time.Sleep(time.Duration(*durationSec) * time.Second)
	close(stopChan)
	wg.Wait()

	totalTime := time.Since(startTime).Seconds()

	// Compute statistics
	latMux.Lock()
	collectedLats := append([]float64(nil), latencies...)
	latMux.Unlock()

	var sumLat float64
	minLat := 0.0
	maxLat := 0.0
	if len(collectedLats) > 0 {
		minLat = collectedLats[0]
		for _, l := range collectedLats {
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
	if len(collectedLats) > 0 {
		avgLat = sumLat / float64(len(collectedLats))
	}

	p50 := percentile(collectedLats, 50)
	p95 := percentile(collectedLats, 95)
	p99 := percentile(collectedLats, 99)
	throughput := float64(totalSent) / totalTime

	fmt.Println("\n=================== BENCHMARK RESULTS ===================")
	fmt.Printf("🎯 Target URL:              %s\n", *targetURL)
	fmt.Printf("👥 Concurrent Clients:      %d\n", *numClients)
	fmt.Printf("⏱  Total Duration:          %.2f s\n", totalTime)
	fmt.Printf("📤 Total Messages Sent:     %d\n", totalSent)
	fmt.Printf("📥 Total Messages Recv:     %d\n", totalReceived)
	fmt.Printf("⚡ Throughput:              %.2f msg/sec\n", throughput)
	fmt.Printf("❌ Total Errors/Drops:      %d\n", totalErrors)
	fmt.Println("----------------- Latency Distribution -----------------")
	fmt.Printf("📉 Min Latency:             %.2f ms\n", minLat)
	fmt.Printf("📊 Avg Latency:             %.2f ms\n", avgLat)
	fmt.Printf("📊 p50 (Median):            %.2f ms\n", p50)
	fmt.Printf("⚠️  p95:                     %.2f ms\n", p95)
	fmt.Printf("🚨 p99:                     %.2f ms\n", p99)
	fmt.Printf("📈 Max Latency:             %.2f ms\n", maxLat)
	fmt.Println("=========================================================\n")
}
