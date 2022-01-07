import json
import time
import asyncio
import websockets
import pprint

async def hello():
    async def get():
        msg = json.loads(await websocket.recv())
        pprint.pprint(msg)

    async with websockets.connect("ws://localhost:10203/ws") as websocket:
        await websocket.send("a-aafcb3d0a44b1af3a0d4da209ba98b88")
        await websocket.send(json.dumps({"kind": "getSchema"}))
        await get()
        #await websocket.send(json.dumps({"kind": "append", "table": "JobStatus", "row": {"job_id": 50444, "status": "active"}}))
        #await get()
        await websocket.send(json.dumps({
            "kind": "query",
            "subscription": "JobStatuses",
            #"cursor": 4,
            "filterCursors": [[12345, 5], [50443, 13]],
        }))
        await get()

asyncio.run(hello())
