import asyncio
import json
from aiohttp.test_utils import AioHTTPTestCase
from api import create_app


class Integration(AioHTTPTestCase):
    async def get_application(self):
        return create_app()

    async def test_dashboard_and_catalog(self):
        for path in ("/", "/app.js", "/style.css"):
            async with self.client.get(path) as response:
                self.assertEqual(response.status, 200)
                self.assertTrue(await response.read())
        response = await self.client.get("/api/catalog")
        self.assertEqual(len(await response.json()), 7)

    async def test_demo_live_stream_and_reconnect(self):
        response = await self.client.post("/api/demo", json={})
        self.assertEqual(response.status, 202)
        writer, waiting, independent = (await response.json())["ids"]
        async with asyncio.timeout(7):
            async with self.client.get("/api/events") as stream:
                async def frame():
                    line = await stream.content.readline()
                    await stream.content.readline()
                    return {r["id"]:r for r in json.loads(line.decode()[6:])}
                state = await frame()
                self.assertEqual(state[waiting]["state"], "WAITING")
                self.assertIn(f"#{writer}", state[waiting]["blocker"])
                self.assertEqual(state[independent]["result"], 9)
                self.assertEqual(state[writer]["state"], "RUNNING")
                while state[waiting]["state"] != "COMPLETED":
                    state = await frame()
                self.assertEqual(state[waiting]["result"], 30)
            snapshot = await (await self.client.get("/api/runs")).json()
            self.assertEqual(len(snapshot), 3)
            async with self.client.get("/api/events") as fresh:
                self.assertEqual(json.loads((await fresh.content.readline())[6:]), snapshot)

    async def test_invalid_requests_and_failed_run(self):
        for body in ({"trigger":"scheduled","process":"P1","day":"2026-11-20"},
                     {"process":"X","day":"2026-11-20"}, {"process":"P2","day":"invalid"}):
            self.assertEqual((await self.client.post("/api/runs", json=body)).status, 400)
        for day in ("2020-01-01","2026-11-19"):
            response = await self.client.post("/api/runs", json={"process":"P2","day":day})
            self.assertEqual(response.status, 202)
        good, bad = await (await self.client.get("/api/runs")).json()
        self.assertEqual((good["result"],bad["state"]), (9,"FAILED"))
        self.assertEqual(good["trigger"], "manual")


class ClockIntegration(AioHTTPTestCase):
    async def get_application(self):
        return create_app(interval=.05)

    async def test_timer_reaches_engine(self):
        async with asyncio.timeout(2):
            while not (rows := await (await self.client.get("/api/runs")).json()):
                await asyncio.sleep(.01)
        self.assertEqual((rows[0]["trigger"], rows[0]["result"]), ("scheduled", 30))

class ConflictIntegration(AioHTTPTestCase):
    async def get_application(self):
        return create_app()

    async def check_conflict(self, scenario, reader, owner, source, queued=False):
        response = await self.client.post("/api/demo", json={"scenario": scenario})
        self.assertEqual(response.status, 202)
        ids = (await response.json())["ids"]
        async with asyncio.timeout(10):
            while True:
                rows = {r["id"]:r for r in await (await self.client.get("/api/runs")).json()}
                if all(rows[i]["state"] == "COMPLETED" for i in ids):
                    break
                await asyncio.sleep(.2)
        wait = rows[ids[reader]]["waits"][0]
        self.assertEqual((wait["owner"],wait["resource"][0],wait["queued"]),
                         (ids[owner],source,queued))
        return wait

    async def test_writers(self):
        wait = await self.check_conflict("writers",1,0,"C")
        self.assertEqual((wait["mode"],wait["owner_mode"]),("write","write"))

    async def test_window(self):
        wait = await self.check_conflict("window",1,0,"A")
        self.assertEqual(wait["resource"][1],"2026-11-19")

    async def test_queue(self):
        await self.check_conflict("queue",2,1,"A",True)

class ManualNumbers(AioHTTPTestCase):
    async def get_application(self):
        return create_app()

    async def test_publish_and_read_custom_date(self):
        for process, extra in [("P1", {"values":[8,-3,20]}), ("P2", {})]:
            response = await self.client.post("/api/runs", json={"process":process,"day":"2027-01-03",**extra})
            self.assertEqual(response.status,202)
        rows = await (await self.client.get("/api/runs")).json()
        self.assertEqual([r["result"] for r in rows],[25,25])
        for r in rows:
            times = [e[1] for e in r["timing"]]
            self.assertEqual(times, sorted(times))
        for values in ([], [False], [1000000001]):
            response = await self.client.post("/api/runs", json={"process":"P1","day":"2027-01-03","values":values})
            self.assertEqual(response.status,400)
