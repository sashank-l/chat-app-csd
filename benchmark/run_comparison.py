import argparse
import asyncio
import json
import statistics
import time
import websockets


async def run_single_client(url, client_id, duration, interval, results, stop_event):
    latencies = []
    sent = 0
    recv = 0
    try:
        async with websockets.connect(url, ping_interval=None) as ws:
            await ws.send(json.dumps({"type": "join", "username": client_id}))
            
            async def receiver():
                nonlocal recv
                try:
                    while not stop_event.is_set():
                        await ws.recv()
                        recv += 1
                except Exception:
                    pass

            recv_task = asyncio.create_task(receiver())

            msg_count = 0
            while not stop_event.is_set():
                msg_count += 1
                t0 = time.perf_counter()
                payload = {
                    "type": "message",
                    "text": f"Load test message {msg_count} from {client_id}",
                    "timestamp": int(time.time() * 1000)
                }
                await ws.send(json.dumps(payload))
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                latencies.append(elapsed_ms)
                sent += 1
                await asyncio.sleep(interval)

            recv_task.cancel()
    except Exception:
        pass
    finally:
        results.append((sent, recv, latencies))


async def benchmark(url, clients, duration, interval):
    stop_event = asyncio.Event()
    results = []
    tasks = [
        asyncio.create_task(run_single_client(url, f"user_{i:03d}", duration, interval, results, stop_event))
        for i in range(1, clients + 1)
    ]
    t0 = time.perf_counter()
    await asyncio.sleep(duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    total_time = time.perf_counter() - t0

    all_lats = []
    total_sent = 0
    total_recv = 0
    for s, r, lats in results:
        total_sent += s
        total_recv += r
        all_lats.extend(lats)

    all_lats.sort()
    throughput = total_sent / total_time if total_time > 0 else 0
    avg_lat = statistics.mean(all_lats) if all_lats else 0
    p50_lat = statistics.median(all_lats) if all_lats else 0
    p95_lat = all_lats[int(len(all_lats) * 0.95)] if all_lats else 0
    p99_lat = all_lats[int(len(all_lats) * 0.99)] if all_lats else 0

    return {
        "clients": clients,
        "duration": total_time,
        "sent": total_sent,
        "recv": total_recv,
        "throughput": throughput,
        "avg_lat": avg_lat,
        "p50_lat": p50_lat,
        "p95_lat": p95_lat,
        "p99_lat": p99_lat,
    }


def main():
    parser = argparse.ArgumentParser(description="Run Automated Benchmarks and Output Comparison Table")
    parser.add_argument("--url", default="ws://10.1.75.51:4209/ws", help="Load Balancer WebSocket URL")
    parser.add_argument("--clients", type=int, default=50, help="Number of concurrent virtual clients")
    parser.add_argument("--duration", type=int, default=20, help="Benchmark duration in seconds")
    parser.add_argument("--interval", type=float, default=0.15, help="Sending interval in seconds")
    args = parser.parse_args()

    print(f"🚀 Running Benchmark ({args.clients} clients, {args.duration}s)...")
    res = asyncio.run(benchmark(args.url, args.clients, args.duration, args.interval))

    print("\n" + "=" * 50)
    print("📊 BENCHMARK METRICS SUMMARY")
    print("=" * 50)
    print(f"Throughput:       {res['throughput']:.2f} msg/sec")
    print(f"Total Sent:       {res['sent']} messages")
    print(f"Avg Latency:      {res['avg_lat']:.2f} ms")
    print(f"p50 (Median):     {res['p50_lat']:.2f} ms")
    print(f"p95 Latency:      {res['p95_lat']:.2f} ms")
    print(f"p99 Latency:      {res['p99_lat']:.2f} ms")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
