import time
import asyncio
import websockets

async def hello():
    async with websockets.connect("ws://localhost:10203/ws") as websocket:
        await websocket.send("foo")
        while True:
            await websocket.send('{"kind": "ping"}')
            msg = await websocket.recv()
            print("Got back:", msg)
            break

asyncio.run(hello())
