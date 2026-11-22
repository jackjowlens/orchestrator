"""Single-event-loop scheduler for trusted asynchronous partition computations."""
import asyncio
import re
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType


@dataclass(frozen=True, order=True)
class Slice:
    source: str
    day: date

    def __post_init__(self):
        if type(self.source) is not str or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", self.source):
            raise ValueError("invalid source")
        if type(self.day) is not date:
            raise ValueError("expected a calendar date")


type Values = tuple[int, ...]
type Worker = Callable[[Mapping[Slice, Values]], Awaitable[tuple[int, dict[Slice, Values]]]]


def validate_data(data: dict[Slice, Values]):
    for key, values in data.items():
        if type(key) is not Slice or type(values) is not tuple or any(type(v) is not int for v in values):
            raise ValueError("expected Slice -> immutable tuple of integers")


@dataclass(frozen=True)
class Request:
    process: str
    actor: str
    trigger: str
    reads: frozenset[Slice]
    writes: frozenset[Slice]

    def __post_init__(self):
        if any(type(s) is not str or not s.strip() or len(s) > 128 for s in (self.process, self.actor)):
            raise ValueError("invalid process or actor")
        if self.trigger not in ("manual", "scheduled"):
            raise ValueError("unknown trigger")
        for group in (self.reads, self.writes):
            if type(group) is not frozenset or any(type(k) is not Slice for k in group):
                raise ValueError("expected an immutable set of slices")
        if not 1 <= len(self.reads | self.writes) <= 64:
            raise ValueError("expected 1 to 64 slices")

    def conflicts(self, other):
        return (self.writes & (other.reads | other.writes)) | (self.reads & other.writes)


@dataclass(frozen=True)
class Event:
    sequence: int
    run: int
    request: Request
    state: str
    # One concrete (blocking run, conflicting slice) per waiting event.
    blocker: tuple[int, Slice] | None = None


class Engine:
    def __init__(self, initial: dict[Slice, Values], *, capacity=256, history=2048):
        if any(type(n) is not int or n < 1 for n in (capacity, history)):
            raise ValueError("positive limits required")
        self._data = dict(initial)
        validate_data(self._data)
        self._capacity = capacity
        self._events = deque(maxlen=history)
        self._pending: dict[int, Request] = {}
        self._active: dict[int, Request] = {}
        self._finished: dict[int, asyncio.Event] = {}
        self._sequence = self._last_run = 0

    @property
    def events(self):
        return tuple(self._events)

    def _emit(self, run, req, state, blocker=None):
        self._sequence += 1
        self._events.append(Event(self._sequence, run, req, state, blocker))

    def _rank(self, run, req):
        # Ties favor FIFO: a scheduled arrival can pass four recent manual arrivals.
        return (run - (5 if req.trigger == "scheduled" else 0), run)

    def _blocker(self, run, req):
        for owner, other in self._active.items():
            if overlap := req.conflicts(other):
                return owner, min(overlap)
        for earlier, other in self._pending.items():
            if self._rank(earlier, other) < self._rank(run, req):
                if overlap := req.conflicts(other):
                    return earlier, min(overlap)
        return None

    async def execute(self, req: Request, worker: Worker) -> int:
        """Call from the owning event loop. Worker selection belongs to trusted code."""
        if type(req) is not Request or not callable(worker):
            raise ValueError("expected a validated request and a worker")
        if len(self._pending) + len(self._active) >= self._capacity:
            raise BufferError("admission limit reached")
        self._last_run += 1
        run = self._last_run
        self._finished[run] = asyncio.Event()
        self._pending[run] = req
        self._emit(run, req, "QUEUED")
        try:
            previous = None
            while blocker := self._blocker(run, req):
                if blocker != previous:
                    self._emit(run, req, "WAITING", blocker)
                    previous = blocker
                await self._finished[blocker[0]].wait()
        except asyncio.CancelledError:
            del self._pending[run]
            self._emit(run, req, "CANCELLED")
            self._finished.pop(run).set()
            raise
        del self._pending[run]
        self._active[run] = req
        self._emit(run, req, "RUNNING")

        # Admission, publication and release contain no suspension points.
        # Independent workers can publish during the await. Merge into current data.
        try:
            inputs = MappingProxyType({key: self._data[key] for key in req.reads})
            result, changes = await worker(inputs)
            if type(result) is not int or type(changes) is not dict or changes.keys() != req.writes:
                raise ValueError("worker output differs from declared contract")
            validate_data(changes)
            self._data.update(changes)
            self._emit(run, req, "COMPLETED")
            return result
        except asyncio.CancelledError:
            self._emit(run, req, "CANCELLED")
            raise
        except Exception:
            self._emit(run, req, "FAILED")
            raise
        finally:
            del self._active[run]
            self._finished.pop(run).set()

