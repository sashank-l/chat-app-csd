import argparse
import asyncio
import json
import statistics
import time
import websockets


async def client_worker(
    client_id: str,
    url: str,
    duration: int,
    interval: float,
    latencies: list,
    stats: dict,
    stop_event: asyncio.Event,
):
    try:
        async with websockets.connect(url, ping_interval=None) as ws:
            # Send join
            await ws.send(json.dumps({"type": "join", "username": client_id}))
            stats["connected"] += 1

            # Background receiver
            async def receiver():
                try:
                    while not stop_event.is_set():
                        msg = await ws.recv()
                        stats["received"] += 1
                except Exception:
                    pass

            recv_task = asyncio.create_task(receiver())

            # Sender loop
            msg_count = 0
            while not stop_event.is_set():
                msg_count += 1
                t0 = time.perf_counter()
                payload = {
                    "type": "message",
                    "text": f"Bench msg {msg_count} from {client_id}",
                    "timestamp": int(time.time() * 1000),
                }
                await ws.send(json.dumps(payload))
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                latencies.append(elapsed_ms)
                stats["sent"] += 1
                await asyncio.sleep(interval)

            recv_task.cancel()

    except Exception as e:
        stats["errors"] += 1


async def main():
    parser = argparse.ArgumentParser(description="WebSocket Load Generator for Chat LB")
    parser.add_argument("--url", default="ws://10.1.75.51:4209/ws", help="Load Balancer WebSocket URL")
    parser.add_argument("--clients", type=int, default=50, help="Number of concurrent virtual clients")
    parser.add_argument("--duration", type=int, default=20, help="Benchmark duration in seconds")
    parser.add_argument("--interval", type=float, default=0.2, help="Sending interval in seconds per client")
    args = parser.parse_args()

    print("==========================================================")
    print(f"🔥 Starting Load Generator against: {args.url}")
    print(f"👥 Concurrent Clients: {args.clients} | ⏱ Duration: {args.duration}s | 📨 Interval: {args.interval}s")
    print("==========================================================")

    latencies = []
    stats = {"sent": 0, "received": 0, "errors": 0, "connected": 0}
    stop_event = asyncio.Event()

    tasks = [
        asyncio.create_task(
            client_worker(
                f"bot_{i:03d}",
                args.url,
                args.duration,
                args.interval,
                latencies,
                stats,
                stop_event,
            )
        )
        for i in range(1, args.clients + 1)
    ]

    t_start = time.perf_counter()
    await asyncio.sleep(args.duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    total_time = time.perf_counter() - t_start

    throughput = stats["sent"] / total_time if total_time > 0 else 0

    if latencies:
        latencies.sort()
        p50 = statistics.median(latencies)
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg = statistics.mean(latencies)
        min_l = min(latencies)
        max_l = max(latencies)
    else:
        p50 = p95 = p99 = avg = min_l = max_l = 0

    print("\n=================== BENCHMARK RESULTS ===================")
    print(f"🎯 Target URL:              {args.url}")
    print(f"👥 Concurrent Clients:      {args.clients}")
    print(f"⏱  Total Duration:          {total_time:.2f} s")
    print(f"📤 Total Messages Sent:     {stats['sent']}")
    print(f"📥 Total Messages Recv:     {stats['received']}")
    print(f"⚡ Throughput:              {throughput:.2f} msg/sec")
    print(f"❌ Total Errors/Drops:      {stats['errors']}")
    print("----------------- Latency Distribution -----------------")
    print(f"📉 Min Latency:             {min_l:.2f} ms")
    print(f"📊 Avg Latency:             {avg:.2f} ms")
    print(f"📊 p50 (Median):            {p50:.2f} ms")
    print(f"⚠️  p95:                     {p95:.2f} ms")
    print(f"🚨 p99:                     {p99:.2f} ms")
    print(f"📈 Max Latency:             {max_l:.2f} ms")
    print("=========================================================\n")


if __name__ == "__main__":
    asyncio.run(main())
