"""Abstract processes. Input and output positions use (source, day offset)."""
from dataclasses import dataclass
from datetime import date, timedelta
from collections.abc import Callable

from core import Request, Slice, Values, Worker


def update():
    return 30, ((10, 20),)


def total(a):
    return sum(a), ()


def aggregate(a, b):
    value = sum(a) + sum(b)
    return value, ((value,),)


def delta(previous, current):
    value = sum(current) - sum(previous)
    return value, ((value,),)


def increment(a):
    values = tuple(x + 1 for x in a)
    return sum(values), (values,)


def split(a):
    return len(a), (tuple(x for x in a if x < 0), tuple(x for x in a if x >= 0))


def average(a):
    if not a:
        raise ValueError("empty sample")
    return sum(a) // len(a), ()


@dataclass(frozen=True)
class Process:
    inputs: tuple
    outputs: tuple
    calculate: Callable

    def bind(self, name, day, actor, trigger):
        def resolve(spec):
            return tuple(Slice(source, day + timedelta(days=offset)) for source, offset in spec)

        inputs, outputs = resolve(self.inputs), resolve(self.outputs)
        req = Request(name, actor, trigger, frozenset(inputs), frozenset(outputs))

        async def worker(snapshot):
            result, values = self.calculate(*(snapshot[key] for key in inputs))
            return result, dict(zip(outputs, values, strict=True))

        return req, worker


A, B, C = ("A", 0), ("B", 0), ("C", 0)
CATALOG = {
    "P1": Process((), (A,), update),
    "P2": Process((A,), (), total),
    "P3": Process((A, B), (C,), aggregate),
    "P4": Process((("A", -1), A), (("Delta", 0),), delta),
    "P5": Process((A,), (A,), increment),
    "P6": Process((A,), (("Negative", 0), ("Nonnegative", 0)), split),
    "P7": Process((A,), (), average),
}


def bind(process: str, day: date, actor: str, trigger="manual") -> tuple[Request, Worker]:
    if type(day) is not date:
        raise ValueError("expected calendar date")
    return CATALOG[process].bind(process, day, actor, trigger)
