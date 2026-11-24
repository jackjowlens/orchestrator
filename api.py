import asyncio
import hashlib
import hmac
import secrets
from time import perf_counter_ns
from scheduler import periodic
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from aiohttp import web
from core import Engine, Slice
from processes import CATALOG, bind

ROOT = Path(__file__).parent / "frontend"
DAY = date(2026, 11, 20)
NAMES = dict(zip(CATALOG, ("Обновление", "Сумма", "Сводка", "Разница за сутки",
                           "Приращение", "Разбиение по знаку", "Среднее"), strict=True))


class Service:
    def __init__(self):
        self.rows = OrderedDict()
        self.pending = {}
        self.jobs = set()
        self.changed = asyncio.Event()
        data = {Slice("A", DAY): (1, 2), Slice("A", DAY - timedelta(days=1)): (4, 5),
                Slice("B", DAY): (7,), Slice("B", DAY - timedelta(days=1)): (6,)}
        self.engine = Engine(data, on_event=self.record)

    def notify(self):
        self.changed.set()
        self.changed = asyncio.Event()

    def record(self, event):
        if event.state == "QUEUED":
            row, admitted = self.pending.pop(id(event.request))
            row["id"] = event.run
            self.rows[event.run] = row
            admitted.set_result(event.run)
        row = self.rows[event.run]
        blocker = None
        if event.blocker:
            owner, key = event.blocker
            blocker = f"#{owner} · {key.source} · {key.day}"
            other = self.rows[owner]
            resource = [key.source, str(key.day)]
            row["waits"].append(dict(owner=owner, process=other["process"], resource=resource,
                                     mode="write" if key in event.request.writes else "read",
                                     owner_mode="write" if resource in other["writes"] else "read",
                                     queued=other["state"] != "RUNNING"))
        row["timing"].append([event.state, perf_counter_ns() // 1000])
        stamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        row.update(state=event.state, blocker=blocker)
        row["events"].append([stamp, event.state, blocker])
        self.notify()

    async def submit(self, process, day, actor, trigger="manual", delay=0, values=None):
        req, worker = bind(process, day, actor, trigger)
        if values is not None:
            if process != "P1" or type(values) is not list or not 1 <= len(values) <= 128:
                raise ValueError("invalid input series")
            if any(type(n) is not int or abs(n) > 10**9 for n in values):
                raise ValueError("invalid input number")
            series = tuple(values)
        admitted = asyncio.get_running_loop().create_future()
        row = dict(process=process, day=str(day), actor=actor, trigger=trigger, events=[], hold=delay,
                   result=None, error=None, waits=[], timing=[], values=values,
                   reads=[[k.source, str(k.day)] for k in sorted(req.reads)],
                   writes=[[k.source, str(k.day)] for k in sorted(req.writes)])
        self.pending[id(req)] = row, admitted

        async def compute(inputs):
            if delay:
                await asyncio.sleep(delay)
            if values is not None:
                return sum(series), {next(iter(req.writes)): series}
            return await worker(inputs)

        async def execute():
            try:
                row["result"] = await self.engine.execute(req, compute)
            except Exception as exc:
                if not admitted.done():
                    admitted.set_exception(exc)
                row["error"] = str(exc)
            finally:
                self.pending.pop(id(req), None)
                if len(self.rows) > 400:
                    for key in list(self.rows):
                        if self.rows[key]["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
                            del self.rows[key]
                            if len(self.rows) <= 400:
                                break
                self.notify()

        task = asyncio.create_task(execute())
        self.jobs.add(task)
        task.add_done_callback(self.jobs.discard)
        return await asyncio.shield(admitted)


def create_app(interval=60):
    app = web.Application(client_max_size=4096)
    service = Service()
    secret = secrets.token_bytes(32)

    def sign(actor):
        return hmac.new(secret, actor.encode(), hashlib.sha256).hexdigest()

    async def catalog(request):
        return web.json_response([dict(id=k, name=NAMES[k], reads=v.inputs, writes=v.outputs)
                                  for k, v in CATALOG.items()])

    def snapshot():
        return list(reversed(service.rows.values()))

    async def runs(request):
        return web.json_response(snapshot())

    async def submit(request):
        if request.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType()
        try:
            body = await request.json()
            if type(body) is not dict or not {"process", "day"} <= set(body) <= {"process", "day", "values", "hold"}:
                raise ValueError()
            day = date.fromisoformat(body["day"])
            actor, _, signature = request.cookies.get("operator", "").partition(".")
            if len(actor) != 12 or not signature.isascii() or not hmac.compare_digest(signature, sign(actor)):
                actor = uuid4().hex[:12]
            hold = body.get("hold", 0)
            if type(hold) is not int or hold not in (0, 3, 8):
                raise ValueError()
            run = await service.submit(body["process"], day, actor, delay=hold, values=body.get("values"))
        except (ValueError, KeyError, TypeError, OverflowError):
            raise web.HTTPBadRequest(text="Проверьте процесс и дату.")
        except BufferError:
            raise web.HTTPTooManyRequests()
        response = web.json_response({"id": run}, status=202)
        response.set_cookie("operator", actor + "." + sign(actor), httponly=True, samesite="Strict")
        return response

    demo_lock = asyncio.Lock()

    async def demo(request):
        if request.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType()
        try:
            body = await request.json()
            scenario = body.get("scenario", "refresh")
            yesterday = DAY - timedelta(days=1)
            plans = {
                "refresh": [("P1", DAY, 3), ("P2", DAY, 0), ("P2", yesterday, 0)],
                "window": [("P5", yesterday, 4), ("P4", DAY, 0), ("P2", DAY, 0)],
                "writers": [("P3", DAY, 4), ("P3", DAY, 0)],
                "queue": [("P2", DAY, 4), ("P5", DAY, 2), ("P2", DAY, 0)],
            }
            plan = plans[scenario]
        except (ValueError, AttributeError, KeyError, TypeError):
            raise web.HTTPBadRequest(text="Неизвестный сценарий.")
        if demo_lock.locked() or service.jobs:
            raise web.HTTPConflict(text="Дождитесь завершения активных запусков.")
        async with demo_lock:
            ids = []
            for index, (process, day, delay) in enumerate(plan):
                trigger = "manual" if index else "scheduled"
                actor = f"оператор-{index}" if index else "timer"
                ids.append(await service.submit(process, day, actor, trigger, delay))
        return web.json_response({"ids": ids}, status=202)

    async def stream(request):
        import json
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream","Cache-Control": "no-cache"})
        await response.prepare(request)
        try:
            while True:
                changed = service.changed
                payload = json.dumps(snapshot(), ensure_ascii=False)
                await response.write(f"data: {payload}\n\n".encode())
                try:
                    await asyncio.wait_for(changed.wait(), 15)
                except TimeoutError:
                    pass
        except ConnectionResetError:
            pass
        return response

    async def static(request):
        filename = request.path.lstrip("/") or "index.html"
        return web.FileResponse(ROOT / filename)

    async def cleanup(app):
        jobs = list(service.jobs)
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)

    for path in ("/", "/style.css", "/app.js"):
        app.router.add_get(path, static)
    app.router.add_get("/api/catalog", catalog)
    app.router.add_get("/api/runs", runs)
    app.router.add_post("/api/runs", submit)
    app.router.add_post("/api/demo", demo)
    app.router.add_get("/api/events", stream)
    async def clock(app):
        async def tick():
            await service.submit("P1", DAY, "timer", "scheduled")
        timer = asyncio.create_task(periodic(interval, tick))
        try:
            yield
        finally:
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)

    app.cleanup_ctx.append(clock)
    app.on_cleanup.append(cleanup)
    return app


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    web.run_app(create_app(), host="127.0.0.1", port=parser.parse_args().port, handler_cancellation=True)
