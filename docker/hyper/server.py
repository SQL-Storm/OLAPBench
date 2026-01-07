import datetime
import math
import os
import tempfile
import threading
import time

import simplejson as json
import tableauhyperapi
import uvicorn
from fastapi import FastAPI

mem = int(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') * 0.8)
print(f"Hyper server starting (version: {tableauhyperapi.__version__})...")
print(f"Hyper uses {mem / 1024**3:.2f}GB of memory")

app = FastAPI()


def sql_encoder(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, tableauhyperapi.date.Date):
        obj = obj.to_date().isoformat()
    if isinstance(obj, tableauhyperapi.timestamp.Timestamp):
        obj = obj.to_datetime().isoformat()
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return str(obj)
    if isinstance(obj, str):
        return obj
    raise TypeError("Type %s not serializable" % type(obj))


db_dir = "/db"
results_path = os.path.join(db_dir, "results.json")

db_lock = threading.Lock()  # Prevents concurrent write conflicts
result_dir = tempfile.TemporaryDirectory(dir=db_dir)

parameters = {
    "log_dir": result_dir.name,
    "plan_cache_size": "0",
    "memory_limit": str(mem),
}
hyper = tableauhyperapi.HyperProcess(telemetry=tableauhyperapi.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=parameters)
conn = tableauhyperapi.Connection(endpoint=hyper.endpoint, database=os.path.join(result_dir.name, "db.hyper"), create_mode=tableauhyperapi.CreateMode.CREATE_AND_REPLACE)


@app.post("/query")
async def execute_query(payload: dict):
    query = payload.get("query")
    timeout = int(payload.get("timeout", 0))
    fetch = bool(payload.get("fetch", False))
    fetch_limit = int(payload.get("limit", 0))

    if not query:
        return {"rows": -1, "error": "no query provided", "client_total": math.nan, "total": None, "execution": None, "compilation": None}

    with db_lock:  # Ensure thread safety
        timer = None
        if timeout > 0:
            timer = threading.Timer(timeout, conn.cancel)
            timer.start()

        result = []
        rows = -1
        error_message = None
        columns = []

        begin = time.time()
        try:
            cursor = conn.execute_query(query=query.strip())
            
            # Extract column names
            columns = [col.name for col in cursor.schema.columns]
            
            result = cursor.fetchall()

            if fetch:
                rows = len(result)

                if 0 < fetch_limit < len(result):
                    result = result[:fetch_limit]

            client_total = (time.time() - begin) * 1000
        except Exception as e:
            client_total = (time.time() - begin) * 1000
            result = None
            error_message = str(e)

            if not hyper.is_open or "Hyperd connection terminated unexpectedly" in error_message:
                raise e

    if timer is not None:
        timer.cancel()
        timer.join()

    total = None
    execution = None
    compilation = None
    try:
        with open(os.path.join(result_dir.name, "hyperd.log"), 'r') as log:
            for line in reversed(log.readlines()):
                entry = json.loads(line)

                if entry["k"] == "query-end":
                    value = entry["v"]
                    compilation = value['pre-execution']["parsing-time"] * 1000 + value['pre-execution']["compilation-time"] * 1000
                    execution = value["execution-time"] * 1000
                    total = value["elapsed"] * 1000
                    break
    except Exception:
        pass

    # Log results
    if fetch:
        with open(results_path, "w") as f:
            # Store columns and results separately
            result_data = {
                "columns": columns,
                "results": result
            }
            f.write(json.dumps(result_data, use_decimal=True, default=sql_encoder, allow_nan=True))

    return {"rows": rows, "error": error_message, "client_total": client_total, "total": total, "execution": execution, "compilation": compilation}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5432)
