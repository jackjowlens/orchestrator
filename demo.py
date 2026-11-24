"""Deterministic load scenario on dedicated partitions in the same engine."""
from datetime import timedelta
from functools import partial

from core import Request, Slice


def stress_plan(first_day):
    for offset in range(6):
        day = first_day + timedelta(days=offset)
        keys = tuple(Slice(f"Load{i}", day) for i in range(8))

        async def calculate(inputs, keys, operation):
            if operation == "seed":
                return 828, {key: (100 + i,) for i, key in enumerate(keys)}
            if operation == "write":
                changes = {key: (inputs[key][0] + 1,) for key in keys}
                return sum(value[0] for value in changes.values()), changes
            values = tuple(inputs[key][0] for key in keys)
            if operation == "audit" and values != (102, 103, 106, 107, 108, 109, 108, 109):
                raise ValueError(f"Unexpected final partition values: {values}")
            return sum(values), {}

        def job(name, operation, selected, delay, actor):
            reads = frozenset(selected) if operation != "seed" else frozenset()
            writes = frozenset(selected) if operation in ("seed", "write") else frozenset()
            trigger = "scheduled" if operation == "seed" else "manual"
            request = Request(name, actor, trigger, reads, writes)
            return day, request, partial(calculate, keys=selected, operation=operation), delay

        yield job("Seed", "seed", keys, .7, "timer")
        for i in range(6):
            start = 2 * (i % 3)
            yield job("Update", "write", keys[start:start + 4], .2, f"stress-{i + 1}")
            yield job("Read", "read", keys[i % 4:i % 4 + 3], .06, f"stress-{2*i + 1}")
            yield job("Read", "read", keys[5:], .06, f"stress-{2*i + 2}")
        yield job("Audit", "audit", keys, 0, "audit")
