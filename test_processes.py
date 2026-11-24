import asyncio
import unittest
from datetime import date

from core import Engine, Request, Slice
from processes import bind


DAY = date(2026, 1, 1)
PREV = date(2025, 12, 31)
A = Slice("A", DAY)
B = Slice("B", DAY)


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def read(self, engine, key):
        async def inspect(inputs):
            self.observed = inputs[key]
            return 0, {}
        await engine.execute(Request("inspect", "test", "manual", frozenset({key}), frozenset()), inspect)
        return self.observed

    async def test_split_preserves_duplicates_and_zero(self):
        engine = Engine({A: (0, -2, 4, -2)})
        self.assertEqual(await engine.execute(*bind("P6", DAY, "a")), 4)
        self.assertEqual(await self.read(engine, Slice("Negative", DAY)), (-2, -2))
        self.assertEqual(await self.read(engine, Slice("Nonnegative", DAY)), (0, 4))

    async def test_missing_second_input_preserves_old_output(self):
        c = Slice("C", DAY)
        engine = Engine({A: (1,), c: (99,)})
        with self.assertRaises(KeyError):
            await engine.execute(*bind("P3", DAY, "a"))
        self.assertEqual(engine.events[-1].state, "FAILED")
        self.assertEqual(await self.read(engine, c), (99,))
        complete = Engine({A: (1,), B: (2, 3)})
        self.assertEqual(await complete.execute(*bind("P3", DAY, "a")), 6)
        self.assertEqual(await self.read(complete, c), (6,))

    async def test_empty_average_fails_then_new_run_succeeds(self):
        engine = Engine({A: ()})
        with self.assertRaisesRegex(ValueError, "empty sample"):
            await engine.execute(*bind("P7", DAY, "a"))
        self.assertEqual(engine.events[-1].state, "FAILED")
        await engine.execute(*bind("P1", DAY, "timer", "scheduled"))
        self.assertEqual(await engine.execute(*bind("P7", DAY, "a")), 15)

    async def test_new_year_window_waits_for_previous_day_writer(self):
        engine = Engine({A: (50,), Slice("A", PREV): (1,)})
        entered, release = asyncio.Event(), asyncio.Event()
        req, worker = bind("P1", PREV, "timer", "scheduled")

        async def paused(inputs):
            entered.set()
            await release.wait()
            return await worker(inputs)

        async with asyncio.timeout(3):
            writer = asyncio.create_task(engine.execute(req, paused))
            await entered.wait()
            delta = asyncio.create_task(engine.execute(*bind("P4", DAY, "a")))
            await asyncio.sleep(0)
            self.assertEqual(engine.events[-1].blocker, (1, Slice("A", PREV)))
            self.assertEqual(await engine.execute(*bind("P2", DAY, "b")), 50)
            release.set()
            await writer
            self.assertEqual(await delta, 20)
            self.assertEqual(await self.read(engine, Slice("Delta", DAY)), (20,))

    async def test_repeated_increment_runs_twice(self):
        engine = Engine({A: (0,)})
        values = await asyncio.gather(*(engine.execute(*bind("P5", DAY, "a")) for _ in range(2)))
        self.assertEqual(sorted(values), [1, 2])
        self.assertEqual(await self.read(engine, A), (2,))

    async def test_reader_before_refresh_sees_previous_publication(self):
        engine = Engine({A: (1, 2)})
        self.assertEqual(await engine.execute(*bind("P2", DAY, "a")), 3)
        await engine.execute(*bind("P1", DAY, "timer", "scheduled"))
        self.assertEqual(await engine.execute(*bind("P2", DAY, "a")), 30)
