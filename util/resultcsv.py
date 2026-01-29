import csv
import datetime
import decimal
import math
import os
from statistics import mean, median

import simplejson as json
from dbms.dbms import Result
from queryplan.queryplan import encode_query_plan


def sql_encoder(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return str(obj)
    raise TypeError("Type %s not serializable" % type(obj))


class ResultCSV:
    def __init__(self, filename: str, append: bool = False):
        self.filename = filename
        self.filename_current = filename + "_current"
        self.append = append

        self.fieldnames = ["title", "dbms", "version", "query", "state"]
        self.metrics = ["client_total", "total", "execution", "compilation"]
        for metric in self.metrics:
            self.fieldnames.append(metric)
            self.fieldnames.append(metric + "_mean")
            self.fieldnames.append(metric + "_median")

        self.fieldnames.extend(["rows", "message", "extra", "columns", "result", "plan"])

    def __enter__(self):
        if os.path.exists(self.filename) and self.append:
            self.append = True
        else:
            self.append = False
        self.file = open(self.filename, "a" if self.append else "w")

        self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
        if not self.append:
            self.writer.writeheader()
            self.file.flush()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def start_olap(self, title: str, query: str):
        with open(self.filename_current, "w") as file:
            file.write(f"{title},{query}")

    def olap(self, title: str, dbms: str, version: str, query: str, result: Result):
        row = {
            "title": title,
            "dbms": dbms,
            "version": version,
            "query": query,
            "state": result.state,
            "rows": result.rows,
            "message": result.message.replace("\n", " "),
            "extra": json.dumps(result.extra, allow_nan=True),
            "columns": "" if result.columns is None else json.dumps(result.columns),
            "result": "" if result.result is None else json.dumps(result.result, use_decimal=True, default=sql_encoder, allow_nan=True),
            "plan":  "" if result.plan is None else encode_query_plan(result.plan),
        }

        for metric in self.metrics:
            values = getattr(result, metric)
            row[metric] = json.dumps(values)
            if len(values) == 0:
                values = [float('nan')]
            row[metric + "_mean"] = mean(values)
            row[metric + "_median"] = median(values)

        self.writer.writerow(row)
        self.file.flush()

        try:
            os.remove(self.filename_current)
        except Exception:
            pass
