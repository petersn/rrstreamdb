
import asyncio
import streamdb

hmac_secret = "aafcb3d0a44b1af3a0d4da209ba98b88"
auth_token = streamdb.make_auth_token("rw", 3600, hmac_secret)

url = "ws://localhost:10203/ws"
url = "wss://a2n2.redwoodresearch.org/streamdb/ws"
#url = "ws://a2n2.redwoodresearch.org:10203/streamdb/ws"

async def main():
    import pprint
    stream_db = await streamdb.StreamDB.connect(url, auth_token)
    schema = await stream_db.get_schema()
    pprint.pprint(schema)
    print("\x1b[92mHERE\x1b[0m")
    #r = await stream_db.append("JobStatus", {"job_id": 1, "status": "active"})
    #print("\x1b[92mVALUE\x1b[0m")
    #print(r)
    results = await stream_db.query("S3ObjectsMostRecent", groups={"asdf": 0})
    print("RESULTS:", results)
    await stream_db.close()

loop = asyncio.get_event_loop()
loop.set_debug(True)
asyncio.run(main())
