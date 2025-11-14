import asyncio
import httpx
import time
from collections import Counter
import matplotlib.pyplot as plt

# Config

FUNCTION_URL = "https://cwk-gsb3hrhjgxf9h3cr.italynorth-01.azurewebsites.net/api/generate"

WORKLOADS = [
    1, 2, 5,
    10, 20, 30, 40, 50,
    75, 100, 150, 200, 250, 300,350,400,500,650,700
]

SENSORS_PER_READING = 20
WARMUP_CALLS = 15

# HTTP Request Helper

async def call_function(client: httpx.AsyncClient):
    """
    Sends a single GET request to the Azure Function.
    Returns a label indicating success or error type.
    """
    try:
        resp = await client.get(FUNCTION_URL, timeout=120.0)
        if resp.status_code == 200:
            return "Success"
        return f"Fail_HTTP_{resp.status_code}"
    except httpx.ReadTimeout:
        return "Fail_Timeout"
    except Exception as e:
        return f"Fail_Error_{type(e).__name__}"

# Run Individual Concurrency Level

async def run_test(num_calls: int):
    """
    Runs one scalability test at a specified concurrency level.
    Returns detailed metrics + graphing.
    """
    print(f"\nRunning PARALLEL test: {num_calls} concurrent calls")

    async with httpx.AsyncClient() as client:
        tasks = [call_function(client) for _ in range(num_calls)]
        start = time.time()
        results = await asyncio.gather(*tasks)
        end = time.time()

    duration = end - start

    # Count results
    counts = Counter(results)
    success_count = counts.get("Success", 0)
    fail_count = num_calls - success_count

    print(f"Duration: {duration:.2f} seconds")
    print(f"Total Succeeded: {success_count}")
    print(f"Total Failed: {fail_count}")

    if fail_count > 0:
        print("Failure Details:")
        for reason, count in counts.items():
            if reason != "Success":
                print(f"  - {reason}: {count} times")

    # Throughput
    total_successful_readings = success_count * SENSORS_PER_READING
    tps = total_successful_readings / duration if duration > 0 else 0

    print(f"Throughput (successful): {tps:.2f} readings/sec")

    return duration, total_successful_readings, tps, fail_count

# Warmup Phase

async def warmup():
    """
    Sends a small number of warm-up calls to avoid cold-start.
    """
    print("\n   Warmup Phase")
    async with httpx.AsyncClient() as client:
        tasks = [call_function(client) for _ in range(WARMUP_CALLS)]
        start = time.time()
        results = await asyncio.gather(*tasks)
        end = time.time()
    print(f"Warmup completed ({WARMUP_CALLS} calls, {results.count('Success')} succeeded, {end-start:.2f}s)\n")

# Main Test 

async def main():
    print("     Starting Parallel Scalability Test")
    print(f"Target URL: {FUNCTION_URL}")

    await warmup()

    summary = []
    throughputs = []
    conc_levels = []

    for num_calls in WORKLOADS:
        metrics = await run_test(num_calls)

        # Save results for graph
        conc_levels.append(num_calls)
        throughputs.append(metrics[2])  # throughput value

        # Save full summary line
        summary.append((num_calls,) + metrics)

        await asyncio.sleep(1)

    # Print summary
    print("\n   Summary (concurrency, duration, total_readings, tps, fails) ---")
    for line in summary:
        print(line)

    # Plot Throughput vs Concurrency
    plt.figure(figsize=(9, 5))
    plt.plot(conc_levels, throughputs, marker="o", linewidth=2)
    plt.title("Azure Function Scalability\nThroughput vs Concurrent Requests")
    plt.xlabel("Concurrent Requests")
    plt.ylabel("Throughput (readings/sec)")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()

    print("\n    Test Complete")

if __name__ == "__main__":
    asyncio.run(main())

