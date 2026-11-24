"""Compare conflict-aware admission policies with identical synthetic workloads."""
import asyncio
from datetime import date
from statistics import median
from time import perf_counter_ns

from core import Engine, Request, Slice


A = Slice("A", date(2026, 11, 20))
B = Slice("B", A.day)


async def trial(limit, read_seconds, reader_count):
    times = {}
    bypasses = 0
    first_entered = asyncio.Event()

    def record(event):
        nonlocal bypasses
        times[event.request.actor, event.state] = perf_counter_ns()
        bypasses += len(event.bypassed)

    engine = Engine({A: (1,), B: (2,)}, backfill_limit=limit, on_event=record)

    async def submit(actor, reads=(), writes=(), sleep=0):
        async def worker(inputs):
            if actor == "owner":
                first_entered.set()
            deadline = perf_counter_ns() + int(sleep * 1e9)
            while (remaining := deadline - perf_counter_ns()) > 0:
                await asyncio.sleep(remaining / 1e9)
            return sum(sum(values) for values in inputs.values()), {key: (8,) for key in writes}

        req = Request("benchmark", actor, "manual", frozenset(reads), frozenset(writes))
        return await engine.execute(req, worker)

    async with asyncio.timeout(3):
        owner = asyncio.create_task(submit("owner", writes=[A], sleep=.030))
        await first_entered.wait()
        writer = asyncio.create_task(submit("writer", writes=[A, B], sleep=.010))
        await asyncio.sleep(0)
        readers = [asyncio.create_task(submit(f"reader{i}", reads=[B], sleep=read_seconds))
                   for i in range(reader_count)]
        await asyncio.gather(owner, writer, *readers)
        assert await submit("check", reads=[A, B]) == 16

    def elapsed(actor, start, end):
        return (times[actor, end] - times[actor, start]) / 1e6

    waits = [elapsed(f"reader{i}", "QUEUED", "RUNNING") for i in range(reader_count)]
    finished = max(t for (actor, state), t in times.items() if state == "COMPLETED" and actor != "check")
    early = sum(times[f"reader{i}", "COMPLETED"] < times["owner", "COMPLETED"] for i in range(reader_count))
    return (waits[0], sum(waits) / len(waits), elapsed("writer", "QUEUED", "RUNNING"),
            (finished - times["owner", "QUEUED"]) / 1e6, bypasses, early)


METRICS = ("first_reader_wait_ms", "mean_reader_wait_ms", "writer_wait_ms",
           "makespan_ms", "bypasses", "early_readers")


async def compare(seconds, count, *, repetitions=15):
    samples = {0: [], 2: []}
    for iteration in range(repetitions):
        for limit in ((0, 2) if iteration % 2 == 0 else (2, 0)):
            samples[limit].append(await trial(limit, seconds, count))
    return {limit: dict(zip(METRICS, map(median, zip(*runs)), strict=True))
            for limit, runs in samples.items()}


async def main():
    print("profile, policy, " + ", ".join(METRICS))
    for profile, seconds, count in [("short", .006, 2), ("burst", .006, 12), ("long", .055, 2)]:
        for limit, metrics in (await compare(seconds, count)).items():
            print(f"{profile}, {limit}, " + ", ".join(f"{v:.2f}" for v in metrics.values()))


if __name__ == "__main__":
    asyncio.run(main())
