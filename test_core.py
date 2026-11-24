import asyncio
import unittest
from datetime import date

from core import Engine, Request, Slice


A = Slice("A", date(2026, 11, 20))
B = Slice("B", A.day)
YESTERDAY = Slice("A", date(2026, 11, 19))


def request(read=(), write=(), trigger="manual"):
    return Request("sum", "demo", trigger, frozenset(read), frozenset(write))


async def total(inputs):
    return sum(sum(values) for values in inputs.values()), {}


class Gate:
    """Test-controlled point inside an admitted computation."""
    def __init__(self, changes=None):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.changes = changes or {}

    async def __call__(self, inputs):
        self.entered.set()
        await self.release.wait()
        return 0, self.changes


class CoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = Engine({A: (1, 2), YESTERDAY: (4, 5), B: (7,)})

    def start(self, req, worker):
        return asyncio.create_task(self.engine.execute(req, worker))

    async def test_dates_and_wait_reason(self):
        gate = Gate({A: (10, 20)})
        update = self.start(request(write=[A], trigger="scheduled"), gate)
        await asyncio.wait_for(gate.entered.wait(), 1)
        waiting = self.start(request([A]), total)
        independent = self.start(request([YESTERDAY]), total)
        self.assertEqual(await asyncio.wait_for(independent, 1), 9)
        self.assertFalse(waiting.done())
        self.assertEqual(self.engine.events[3].blocker, (1, A))
        gate.release.set()
        self.assertEqual(await asyncio.wait_for(asyncio.gather(update, waiting), 1), [0, 30])
        self.assertEqual([e.run for e in self.engine.events if e.state == "COMPLETED"], [3, 1, 2])

    async def test_shared_read_and_writer_fairness(self):
        first, second, writer = Gate(), Gate(), Gate({A: (8,)})
        tasks = [self.start(request([A]), g) for g in (first, second)]
        await asyncio.wait_for(asyncio.gather(first.entered.wait(), second.entered.wait()), 1)
        tasks.extend([self.start(request(write=[A]), writer), self.start(request([A]), total)])
        first.release.set()
        second.release.set()
        await asyncio.wait_for(writer.entered.wait(), 1)
        self.assertFalse(tasks[-1].done())
        writer.release.set()
        self.assertEqual((await asyncio.wait_for(asyncio.gather(*tasks), 1))[-1], 8)

    async def test_opposite_resource_order_and_shared_output(self):
        requests = [request([B], [A, B]), request([A], [B, A])]
        gate = Gate({A: (3,), B: (4,)})
        first = self.start(requests[0], gate)
        await asyncio.wait_for(gate.entered.wait(), 1)

        async def second_worker(inputs):
            return inputs[A][0], {B: (8,), A: (7,)}

        second = self.start(requests[1], second_worker)
        await asyncio.sleep(0)
        self.assertEqual(self.engine.events[-1].state, "WAITING")
        gate.release.set()
        self.assertEqual(await asyncio.wait_for(asyncio.gather(first, second), 1), [0, 3])
        self.assertEqual(await self.engine.execute(request([A, B]), total), 15)

    async def test_failure_discards_staged_writes_and_wakes_reader(self):
        gate = Gate()

        async def fails(inputs):
            staged = {A: (99,)}
            await gate(inputs)
            raise RuntimeError("synthetic failure")

        job = self.start(request(write=[A, B]), fails)
        await asyncio.wait_for(gate.entered.wait(), 1)
        reader = self.start(request([A, B]), total)
        gate.release.set()
        with self.assertRaises(RuntimeError):
            await job
        self.assertEqual(await asyncio.wait_for(reader, 1), 10)
        self.assertIn("FAILED", [e.state for e in self.engine.events])

    async def test_cancel_waiter_and_owner(self):
        gate = Gate({A: (100,)})
        owner = self.start(request(write=[A]), gate)
        await asyncio.wait_for(gate.entered.wait(), 1)
        waiter = self.start(request([A]), total)
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        next_reader = self.start(request([A]), total)
        owner.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner
        self.assertEqual(await asyncio.wait_for(next_reader, 1), 3)
        self.assertEqual(sum(e.state == "CANCELLED" for e in self.engine.events), 2)

    async def test_priority_with_bounded_overtaking(self):
        gate = Gate({A: (0,)})
        first = self.start(request(write=[A]), gate)
        await asyncio.wait_for(gate.entered.wait(), 1)

        async def write(inputs):
            return 1, {A: (1,)}

        tasks = [self.start(request(write=[A], trigger=kind), write)
                 for kind in ["manual"] + ["scheduled"] * 8]
        await asyncio.sleep(0)
        gate.release.set()
        await asyncio.wait_for(asyncio.gather(first, *tasks), 1)
        self.assertEqual([e.run for e in self.engine.events if e.state == "RUNNING"],
                         [1, 3, 4, 5, 6, 2, 7, 8, 9, 10])

    async def test_worker_boundary_and_atomic_validation(self):
        async def bad(inputs):
            self.assertEqual(set(inputs), {A})
            with self.assertRaises(TypeError):
                inputs[A] = (0,)
            return 0, {A: (99,), B: [3]}

        with self.assertRaises(ValueError):
            await self.engine.execute(request([A], [A, B]), bad)

        async def undeclared(inputs):
            return 0, {B: (99,)}

        with self.assertRaises(ValueError):
            await self.engine.execute(request([A]), undeclared)
        self.assertEqual(await self.engine.execute(request([A, B]), total), 10)

    async def test_disjoint_writes_survive_overlap(self):
        left, right = Gate({A: (9,)}), Gate({B: (6,)})
        jobs = [self.start(request(write=[k]), g) for k, g in [(A, left), (B, right)]]
        await asyncio.wait_for(asyncio.gather(left.entered.wait(), right.entered.wait()), 1)
        right.release.set()
        await jobs[1]
        left.release.set()
        await jobs[0]
        self.assertEqual(await self.engine.execute(request([A, B]), total), 15)

    async def test_limits_and_history(self):
        self.engine = Engine({A: (1,)}, capacity=1, history=2)
        gate = Gate()
        task = self.start(request([A]), gate)
        await asyncio.wait_for(gate.entered.wait(), 1)
        with self.assertRaises(BufferError):
            await self.engine.execute(request([B]), total)
        gate.release.set()
        await task
        self.assertEqual([e.sequence for e in self.engine.events], [2, 3])

    def test_input_validation(self):
        with self.assertRaises(ValueError):
            Slice("../A", A.day)
        with self.assertRaises(ValueError):
            Slice("A", "2026-11-20")
        with self.assertRaises(ValueError):
            request()
        with self.assertRaises(ValueError):
            request([A], trigger="admin")
        with self.assertRaises(ValueError):
            Engine({A: [1]})


    async def test_invalid_submission_and_missing_input(self):
        with self.assertRaises(ValueError):
            await self.engine.execute(None, total)
        missing = Slice("Missing", A.day)
        with self.assertRaises(KeyError):
            await self.engine.execute(request([missing]), total)
        self.assertEqual(self.engine.events[-1].state, "FAILED")
        self.assertEqual(await self.engine.execute(request([A]), total), 3)

    async def test_parallel_updates_match_serial_replay(self):
        import random

        rng = random.Random(1729)
        keys = [Slice("S" + str(i), A.day) for i in range(8)]
        self.engine = Engine({key: (0,) for key in keys}, history=32768)
        jobs = []
        operations = []
        for index in range(160):
            left, right = rng.sample(keys, 2)
            operations.append((left, right))

            async def compute(inputs, left=left, right=right):
                await asyncio.sleep(0)
                value = inputs[left][0] + inputs[right][0] + 1
                return value, {right: (value,)}

            kind = "scheduled" if index % 3 == 0 else "manual"
            jobs.append(self.start(request([left, right], [right], kind), compute))
        results = await asyncio.wait_for(asyncio.gather(*jobs), 30)
        self.assertEqual(sum(e.state == "COMPLETED" for e in self.engine.events), len(jobs))
        replay = dict.fromkeys(keys, 0)
        for event in self.engine.events:
            if event.state == "COMPLETED":
                left, right = operations[event.run - 1]
                replay[right] += replay[left] + 1
                self.assertEqual(results[event.run - 1], replay[right])
        for key in keys:
            self.assertEqual(await self.engine.execute(request([key]), total), replay[key])


class BackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_bypass_limit_and_write_isolation(self):
        engine = Engine({A: (1,), B: (2,)}, backfill_limit=2)
        owner = Gate({A: (6,)})
        writer = Gate({A: (7,), B: (8,)})
        first = asyncio.create_task(engine.execute(request(write=[A]), owner))
        async with asyncio.timeout(3):
            await owner.entered.wait()
            blocked = asyncio.create_task(engine.execute(request(write=[A, B]), writer))
            await asyncio.sleep(0)
            self.assertEqual(await engine.execute(request([B]), total), 2)
            self.assertEqual(await engine.execute(request([B]), total), 2)
            readers = [asyncio.create_task(engine.execute(request([B]), total)) for _ in range(8)]
            await asyncio.sleep(0)
            self.assertTrue(all(not task.done() for task in readers))
            owner.release.set()
            await writer.entered.wait()
            self.assertTrue(all(not task.done() for task in readers))
            writer.release.set()
            await asyncio.gather(first, blocked)
            self.assertEqual(await asyncio.gather(*readers), [8] * 8)
        self.assertEqual([e.bypassed for e in engine.events if e.bypassed], [(2,), (2,)])

    async def test_only_reads_can_backfill_and_active_writer_stays_exclusive(self):
        engine = Engine({A: (1,), B: (2,)})
        owner = Gate({A: (6,)})
        writer = Gate({A: (7,), B: (8,)})
        later_writer = Gate({B: (9,)})
        first = asyncio.create_task(engine.execute(request(write=[A]), owner))
        async with asyncio.timeout(3):
            await owner.entered.wait()
            blocked = asyncio.create_task(engine.execute(request(write=[A, B]), writer))
            await asyncio.sleep(0)
            last = asyncio.create_task(engine.execute(request(write=[B]), later_writer))
            await asyncio.sleep(0)
            self.assertFalse(later_writer.entered.is_set())
            owner.release.set()
            await writer.entered.wait()
            reader = asyncio.create_task(engine.execute(request([B]), total))
            await asyncio.sleep(0)
            self.assertFalse(reader.done())
            writer.release.set()
            await later_writer.entered.wait()
            later_writer.release.set()
            await asyncio.gather(first, blocked, last)
            self.assertEqual(await reader, 9)
        self.assertFalse(any(e.bypassed for e in engine.events))

class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_observer_failure_does_not_leak_access_or_change_outcome(self):
        def observer(event):
            raise RuntimeError(event.state)

        engine = Engine({A: (1,)}, on_event=observer)

        async def update(inputs):
            return 5, {A: (5,)}

        with self.assertLogs("core", level="ERROR") as messages:
            async with asyncio.timeout(2):
                self.assertEqual(await engine.execute(request(write=[A]), update), 5)
                self.assertEqual(await engine.execute(request([A]), total), 5)
        self.assertEqual(len(messages.records), 6)

    async def test_wait_reason_is_emitted_again_when_pending_owner_starts(self):
        engine = Engine({A: (1,), B: (2,)}, backfill_limit=0)
        owner, writer = Gate({A: (4,)}), Gate({A: (5,), B: (6,)})
        first = asyncio.create_task(engine.execute(request(write=[A]), owner))
        async with asyncio.timeout(3):
            await owner.entered.wait()
            second = asyncio.create_task(engine.execute(request(write=[A, B]), writer))
            await asyncio.sleep(0)
            third = asyncio.create_task(engine.execute(request([B]), total))
            await asyncio.sleep(0)
            owner.release.set()
            await writer.entered.wait()
            await asyncio.sleep(0)
            waits = [e.blocker for e in engine.events if e.run == 3 and e.state == "WAITING"]
            self.assertEqual(waits, [(2, B), (2, B)])
            writer.release.set()
            self.assertEqual((await asyncio.gather(first, second, third))[-1], 6)

    async def test_published_snapshot_remains_consistent_across_commit(self):
        engine = Engine({A: (1,), B: (2,)})
        writer = Gate({A: (10,), B: (20,)})
        reader = Gate()
        snapshot = Request("snapshot", "tester", "manual", frozenset({A, B}), frozenset(), snapshot=True)

        async def held_read(inputs):
            await reader(inputs)
            return await total(inputs)

        update = asyncio.create_task(engine.execute(request(write=[A, B]), writer))
        async with asyncio.timeout(3):
            await writer.entered.wait()
            old = asyncio.create_task(engine.execute(snapshot, held_read))
            await reader.entered.wait()
            self.assertEqual(await engine.execute(snapshot, total), 3)
            writer.release.set()
            await update
            self.assertEqual(await engine.execute(snapshot, total), 30)
            reader.release.set()
            self.assertEqual(await old, 3)
        self.assertEqual([e.version for e in engine.events if e.request.snapshot and e.state == "RUNNING"],
                         [1, 1, 2])
