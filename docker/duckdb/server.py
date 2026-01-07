import datetime
import os
import re
import tempfile
import threading
import time

import duckdb
import simplejson as json
import uvicorn
from fastapi import FastAPI

print(f"DuckDB server starting (version: {duckdb.__version__})...")

app = FastAPI()


def sql_encoder(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return str(obj)
    raise TypeError("Type %s not serializable" % type(obj))


db_dir = "/db"
results_path = os.path.join(db_dir, "results.json")

db_lock = threading.Lock()  # Prevents concurrent write conflicts
result_dir = tempfile.TemporaryDirectory(dir=db_dir)
conn = duckdb.connect(database=":memory:", read_only=False)

conn.execute("SET preserve_insertion_order=false")
conn.execute(f"SET temp_directory ='{result_dir.name}'")

conn.execute('create schema public;')
conn.execute('use memory.public;')


@app.post("/query")
async def execute_query(payload: dict):
    query = payload.get("query")
    timeout = int(payload.get("timeout", 0))
    fetch = bool(payload.get("fetch", False))
    fetch_limit = int(payload.get("limit", 0))

    if not query:
        return {"rows": -1, "error": "no query provided", "client_total": float('nan'), "total": float('nan')}

    with db_lock:  # Ensure thread safety
        profile_output = os.path.join(result_dir.name, "profile.json")
        conn.execute("PRAGMA enable_profiling='json';")
        conn.execute("PRAGMA profile_output='" + profile_output + "';")

        timer = None
        if timeout > 0:
            def interrupt():
                conn.interrupt()

            timer = threading.Timer(timeout, interrupt)
            timer.start()

        result = []
        rows = -1
        error_message = None
        columns = []

        begin = time.time()
        try:
            conn.execute(query=query.strip())

            if fetch:
                # Extract column names from cursor description
                if conn.description:
                    columns = [desc[0] for desc in conn.description]

                if fetch_limit > 0:
                    result = conn.fetchmany(fetch_limit)
                    rows = conn.rowcount
                    if rows == -1:
                        rows = len(result)
                else:
                    result = conn.fetchall()
                    rows = len(result)

            client_total = (time.time() - begin) * 1000
        except Exception as e:
            client_total = (time.time() - begin) * 1000
            result = None
            error_message = str(e)

    if timer is not None:
        timer.cancel()
        timer.join()

    total = None
    try:
        with open(profile_output, 'r') as profile:
            total = float(re.findall(r'result...([0-9.]*)', profile.read())[-1]) * 1000
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
            f.write(json.dumps(result_data, use_decimal=True, default=sql_encoder))

    return {"rows": rows, "error": error_message, "client_total": client_total, "total": total}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5432)
