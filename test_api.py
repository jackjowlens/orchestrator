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

class UpgradeIntegration(AioHTTPTestCase):
    async def get_application(self):
        return create_app()

    async def test_queue_history_distinguishes_pending_from_active(self):
        response = await self.client.post("/api/demo", json={"scenario":"queue"})
        ids = (await response.json())["ids"]
        async with asyncio.timeout(10):
            while True:
                runs = await (await self.client.get("/api/runs")).json()
                reader = next(row for row in runs if row["id"] == ids[2])
                if reader["state"] == "COMPLETED":
                    break
                await asyncio.sleep(.1)
        self.assertEqual([(w["owner"], w["queued"]) for w in reader["waits"]],
                         [(ids[1], True), (ids[1], False)])

    async def test_snapshot_completes_while_standard_reader_waits(self):
        response = await self.client.post("/api/demo", json={"scenario":"refresh"})
        _, waiting, _ = (await response.json())["ids"]
        request = {"process":"P2", "day":"2026-11-20", "snapshot":True}
        response = await self.client.post("/api/runs", json=request)
        self.assertEqual(response.status, 202)
        old_id = (await response.json())["id"]
        rejected = await self.client.post("/api/runs", json=dict(request, process="P1"))
        self.assertEqual(rejected.status, 400)
        async with asyncio.timeout(7):
            while True:
                runs = {r["id"]:r for r in await (await self.client.get("/api/runs")).json()}
                self.assertEqual((runs[old_id]["result"], runs[old_id]["version"]), (3, 1))
                if runs[waiting]["state"] == "COMPLETED":
                    break
                await asyncio.sleep(.1)
        self.assertEqual((runs[waiting]["result"], runs[waiting]["version"]), (30, 2))

class StressIntegration(AioHTTPTestCase):
    async def get_application(self):
        return create_app()

    async def test_stress_completes_and_validates_all_partitions(self):
        response = await self.client.post("/api/demo", json={"scenario": "stress", "hold": 0.4})
        self.assertEqual(response.status, 202)
        ids = (await response.json())["ids"]
        self.assertEqual(len(ids), 120)
        overlapping = await self.client.post("/api/demo", json={"scenario": "stress"})
        self.assertEqual(overlapping.status, 409)
        async with asyncio.timeout(12):
            while True:
                rows = await (await self.client.get("/api/runs")).json()
                batch = [row for row in rows if row["id"] in ids]
                if len(batch) == 120 and all(row["state"] in {"COMPLETED","FAILED","CANCELLED"} for row in batch):
                    break
                await asyncio.sleep(.1)
        self.assertTrue(all(row["state"] == "COMPLETED" for row in batch))
        self.assertEqual({len(row["writes"]) for row in batch}, {0, 4, 8})
        self.assertEqual({row["hold"] for row in batch if row["process"] == "Seed"}, {0.4})
        update = next(row for row in batch if row["process"] == "Update")
        self.assertAlmostEqual(update["hold"], 0.2 * 0.4 / 0.7)
        audits = [row for row in batch if row["process"] == "Audit"]
        self.assertEqual([row["result"] for row in audits], [852] * 6)
        self.assertEqual(len({tuple(key) for row in audits for key in row["reads"]}), 48)
        self.assertGreaterEqual(sum(bool(row["waits"]) for row in batch), 100)

    async def test_custom_conflict_delay_and_validation(self):
        for hold in (0, -1, 9, True, "1", None):
            response = await self.client.post("/api/demo", json={"scenario":"queue", "hold":hold})
            self.assertEqual(response.status, 400)
        response = await self.client.post("/api/demo", json={"scenario":"queue", "hold":1.2})
        self.assertEqual(response.status, 202)
        first, writer, last = (await response.json())["ids"]
        rows = {row["id"]:row for row in await (await self.client.get("/api/runs")).json()}
        self.assertEqual(rows[first]["hold"], 1.2)
        self.assertEqual(rows[writer]["hold"], .6)
        self.assertEqual(rows[last]["hold"], 0)
        self.assertEqual(rows[last]["state"], "WAITING")
