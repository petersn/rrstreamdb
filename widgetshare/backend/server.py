import subprocess
import time
import json
import datetime
import logging
import queue
import threading
import hashlib
import concurrent.futures
from typing import List, Dict, Optional, Any, Union
import sys
import os
import re

import tornado.ioloop
import tornado.web
import tornado.websocket
from sqlalchemy.sql.expression import func

global_io_loop = tornado.ioloop.IOLoop.current()

access_log = logging.getLogger("tornado.access")
access_log.setLevel(logging.DEBUG)
app_log = logging.getLogger("tornado.application")
app_log.setLevel(logging.DEBUG)
gen_log = logging.getLogger("tornado.general")
gen_log.setLevel(logging.DEBUG)


evaluation_results = {}
token_to_code = {}

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

def do_work(code: str):
    start_time = time.monotonic()
    #proc = subprocess.Popen(["python", "evaluate_loss.py", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        os.unlink("../dist/index.js")
    except FileNotFoundError:
        pass
    with open("../src/index.tsx", "w") as f:
        f.write(code)
    subprocess.Popen(["npm", "run", "build"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("Launched subprocess:", proc.pid)
    timeout = False
    try:
        stdout, stderr = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
        timeout = True
    print("Subprocess completed:", proc.pid)
    with open("../dist/index.js") as f:
        compiled = f.read()
    return {
        "stdout": stdout.decode(errors="ignore"),
        "stderr": stderr.decode(errors="ignore"),
        "ret": proc.returncode,
        "elapsed": time.monotonic() - start_time,
        "timeout": timeout,
        "compiled": compiled,
    }


class APIJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


def to_json(sqlalchemy_obj):
    return {
        column.name: getattr(sqlalchemy_obj, column.name)
        for column in sqlalchemy_obj.__table__.columns
    }


class AllowCORS:
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with, Content-Type")
        self.set_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

    def options(self, *args):
        self.set_status(204)
        self.finish()


class CompileHandler(AllowCORS, tornado.web.RequestHandler):
    def post(self):
        request = json.loads(self.request.body)

        assert ("code" in request) != ("token" in request)
        if "code" in request:
            token = hashlib.sha256(request["code"].encode("utf8")).hexdigest()
            if token not in token_to_code:
                token_to_code[token] = request["code"]
        else:
            token = request["token"]

        if token not in evaluation_results:
            evaluation_results[token] = thread_pool.submit(do_work, request["code"])

        future = evaluation_results[token]
        if future.done():
            self.write(json.dumps({
                **future.result(),
                "code": token_to_code[token],
                "done": True,
                "token": token,
            }))
        else:
            self.write(json.dumps({"done": False, "token": token}))


application = tornado.web.Application([
    ("/api/compile", CompileHandler),
])

if __name__ == "__main__":
    print("Launching server")
    # NB: Auth is already provided by the nginx reverse proxy, so no auth here.
    # However, we must therefore bind to "localhost" rather than "0.0.0.0" for security.
    application.listen(int(os.getenv("PORT", 20002)), "localhost")
    global_io_loop.start()
