import json
import time
import random
import asyncio
import websockets
import pprint

async def hello(second):
    async def get():
        msg = json.loads(await websocket.recv())
        pprint.pprint(msg)

    if second:
        await asyncio.sleep(1)
        print("Waking up!")

    async with websockets.connect("ws://localhost:10203/ws") as websocket:
        await websocket.send("a-aafcb3d0a44b1af3a0d4da209ba98b88")
        #await websocket.send(json.dumps({"kind": "getSchema"}))
        #await get()
        #await websocket.send(json.dumps({"kind": "append", "table": "JobStatus", "row": {"job_id": 50444, "status": "active"}}))
        #await get()
        if not second:
            await websocket.send(json.dumps({
                "kind": "subscribe",
                "token": 123,
                "subscription": "JobStatuses",
                #"cursor": 4,
                "filterCursors": [[12345, 5], [50443, 13]],
            }))
            print("Waiting for response...")
            await get()
            print("Done!")
            while True:
                await get()
        else:
            await websocket.send(json.dumps({"kind": "append", "table": "JobStatus", "row": {"job_id": 50443, "status": "active"}}))
            await get()
        #await asyncio.sleep(10)
        #print("Sending new row")
        #await websocket.send(json.dumps({"kind": "append", "table": "JobStatus", "row": {"job_id": 50444, "status": "active"}}))
        #print("Waiting for response...")
        #await get()
        #print("Sleeping")
        #await asyncio.sleep(3)
        #print("Final response")
        #await get()


async def main():
    await asyncio.gather(
        hello(False),
        hello(True),
    )

asyncio.run(
    main(),
)
