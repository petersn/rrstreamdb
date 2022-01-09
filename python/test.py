
import asyncio
import streamdb

hmac_secret = "aafcb3d0a44b1af3a0d4da209ba98b88"
auth_token = streamdb.make_auth_token("rw", 3600, hmac_secret)

async def main():
    import pprint
    stream_db = await streamdb.StreamDB.connect("ws://localhost:10203/ws", auth_token)
    schema = await stream_db.get_schema()
    pprint.pprint(schema)
    r = await stream_db.append("JobStatus", {"job_id": 1, "status": "active"})
    print(r)
    results = await stream_db.query("Foo", cursor=10)
    print("RESULTS:", results)
    await stream_db.close()

asyncio.run(main())
