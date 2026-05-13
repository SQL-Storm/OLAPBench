#!/usr/bin/env python3
"""Serve a webpage that visualizes benchmark results from a CSV file."""

import argparse
import csv
import http.server
import json
import os
import re
import socketserver
import sys
from ast import literal_eval


def parse_list(s):
    if not s:
        return []
    try:
        return literal_eval(s)
    except (ValueError, SyntaxError):
        return []


def parse_extra(s):
    if not s:
        return None
    s = s.strip()
    try:
        return json.loads(s.replace("NaN", "null").replace("nan", "null"))
    except json.JSONDecodeError:
        try:
            return literal_eval(s)
        except (ValueError, SyntaxError):
            return None


def parse_float(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def query_sort_key(q):
    m = re.match(r"(\d+)([a-zA-Z]*)", q)
    if m:
        return (int(m.group(1)), m.group(2), q)
    return (10**9, "", q)


def list_csvs(base_dir):
    entries = []
    try:
        names = os.listdir(base_dir)
    except OSError:
        return entries
    for name in names:
        if not name.endswith(".csv"):
            continue
        full = os.path.join(base_dir, name)
        if not os.path.isfile(full):
            continue
        try:
            mtime = os.path.getmtime(full)
        except OSError:
            continue
        entries.append({
            "name": name,
            "path": os.path.abspath(full),
            "mtime": mtime,
        })
    entries.sort(key=lambda e: -e["mtime"])
    return entries


def safe_path(base_dir, candidate):
    if not candidate:
        return None
    full = os.path.abspath(candidate)
    base = os.path.abspath(base_dir)
    if full != base and not full.startswith(base + os.sep):
        return None
    if not full.endswith(".csv"):
        return None
    if not os.path.isfile(full):
        return None
    return full


def load_results(path):
    rows = []
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("state") != "success":
                continue
            extra = parse_extra(row.get("extra", ""))
            extra_numeric = {}
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if isinstance(v, bool):
                        continue
                    if isinstance(v, (int, float)) and v == v:  # filter NaN
                        extra_numeric[k] = v
            rows.append({
                "system": row["title"],
                "query": row["query"],
                "total": parse_list(row.get("total", "")),
                "total_mean": parse_float(row.get("total_mean")),
                "total_median": parse_float(row.get("total_median")),
                "execution_median": parse_float(row.get("execution_median")),
                "compilation_median": parse_float(row.get("compilation_median")),
                "client_total_median": parse_float(row.get("client_total_median")),
                "extra": extra_numeric,
            })
    return rows


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Benchmark Results</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root {
    --bg: #f4f5f7;
    --card: #ffffff;
    --border: #e3e5e8;
    --text: #1a1d21;
    --muted: #6b7280;
    --accent: #2563eb;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0;
    padding: 1.5rem;
    background: var(--bg);
    color: var(--text);
  }
  h1 { margin: 0 0 1rem; font-size: 1.5rem; }
  h2 { margin: 0 0 0.75rem; font-size: 1.05rem; color: var(--text); }
  .meta { color: var(--muted); font-size: 0.85rem; margin-bottom: 1rem; }
  .card {
    background: var(--card);
    padding: 1rem 1.25rem;
    margin-bottom: 1rem;
    border: 1px solid var(--border);
    border-radius: 8px;
  }
  .controls { display: flex; gap: 1rem; align-items: center; margin-bottom: 0.5rem; flex-wrap: wrap; }
  .controls label { font-size: 0.9rem; display: flex; gap: 0.4rem; align-items: center; }
  select, button {
    padding: 0.35rem 0.55rem;
    font-size: 0.9rem;
    border-radius: 4px;
    border: 1px solid var(--border);
    background: white;
    font-family: inherit;
  }
  button { cursor: pointer; }
  button:hover { background: #f0f1f3; }
  button:disabled { opacity: 0.5; cursor: progress; }
  .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; flex-wrap: wrap; }
  .header h1 { margin: 0; }
  .file-bar {
    display: flex; flex-wrap: wrap; gap: 0.4rem;
    margin-bottom: 1rem;
  }
  .file-bar button {
    padding: 0.3rem 0.6rem;
    font-size: 0.85rem;
    color: var(--text);
  }
  .file-bar button.active {
    background: var(--accent);
    border-color: var(--accent);
    color: white;
  }
  .file-bar button.active:hover { background: #1d4ed8; }
  .row { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  @media (max-width: 1100px) { .row { grid-template-columns: 1fr; } }
  .stats { font-size: 0.85rem; color: var(--muted); margin-top: 0.5rem; }
  .stats strong { color: var(--text); }
</style>
</head>
<body>
  <div class="header">
    <h1>Benchmark Results</h1>
    <div class="meta" id="meta"></div>
  </div>
  <div class="file-bar" id="fileBar"></div>

  <div class="card">
    <h2>End-to-end runtime distribution per system</h2>
    <div id="boxplot" style="width:100%;height:380px;"></div>
  </div>

  <div class="card">
    <h2>Runtime per query</h2>
    <div class="controls">
      <label>Y-axis:
        <select id="barScale">
          <option value="linear">linear</option>
          <option value="log" selected>log</option>
        </select>
      </label>
      <label>Metric:
        <select id="barMetric"></select>
      </label>
    </div>
    <div id="bars" style="width:100%;height:480px;"></div>
  </div>

  <div class="card">
    <h2>System-vs-system comparison</h2>
    <div class="controls">
      <label>A:
        <select id="sysA"></select>
      </label>
      <label>B:
        <select id="sysB"></select>
      </label>
      <label>Metric:
        <select id="cmpMetric"></select>
      </label>
      <span class="stats" id="cmpStats"></span>
    </div>
    <div class="row">
      <div>
        <div id="speedup" style="width:100%;height:420px;"></div>
        <div class="stats">Ratio B/A of <span id="lblMetric1"></span> for <strong id="lblA1"></strong> vs <strong id="lblB1"></strong>. Bars above 1× mean A has the smaller value.</div>
      </div>
      <div>
        <div id="abplot" style="width:100%;height:420px;"></div>
        <div class="stats">Each point is one query. Points below the diagonal favour <strong id="lblA2"></strong>; above favour <strong id="lblB2"></strong>.</div>
      </div>
    </div>
  </div>

<script>
const PALETTE = ["#2563eb","#dc2626","#059669","#d97706","#7c3aed","#0891b2","#db2777","#65a30d","#475569","#ea580c"];

function colorFor(i) { return PALETTE[i % PALETTE.length]; }

function querySortKey(q) {
  const m = q.match(/^(\d+)([a-zA-Z]*)/);
  if (m) return [parseInt(m[1], 10), m[2] || "", q];
  return [1e9, "", q];
}
function compareQ(a, b) {
  const ka = querySortKey(a), kb = querySortKey(b);
  for (let i = 0; i < ka.length; i++) {
    if (ka[i] < kb[i]) return -1;
    if (ka[i] > kb[i]) return 1;
  }
  return 0;
}

let data = [], systems = [], queries = [], sysColor = {};
let currentFile = null, baseDir = "";
let timingMetrics = ["total_median", "total_mean", "execution_median", "compilation_median", "client_total_median"];
let extraMetrics = [];

function basename(p) {
  if (!p) return "";
  const i = Math.max(p.lastIndexOf("/"), p.lastIndexOf("\\"));
  return i >= 0 ? p.slice(i + 1) : p;
}

function updateMeta() {
  const name = basename(currentFile);
  document.getElementById("meta").textContent =
    `${name ? name + " · " : ""}${data.length} successful runs · ${systems.length} systems · ${queries.length} queries`;
}

async function fetchData() {
  data = await fetch("/api/data").then(r => r.json());
  systems = [...new Set(data.map(r => r.system))].sort();
  queries = [...new Set(data.map(r => r.query))].sort(compareQ);
  sysColor = Object.fromEntries(systems.map((s, i) => [s, colorFor(i)]));
  const extraKeys = new Set();
  data.forEach(r => Object.keys(r.extra || {}).forEach(k => extraKeys.add(k)));
  extraMetrics = [...extraKeys].sort();
  updateMeta();
}

let fileBarOnPick = null;

async function fetchFiles() {
  const res = await fetch("/api/files").then(r => r.json());
  currentFile = res.current;
  baseDir = res.base_dir || "";
  const bar = document.getElementById("fileBar");
  bar.innerHTML = "";
  res.files.forEach(f => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = f.name;
    btn.title = f.path;
    if (f.path === currentFile) btn.classList.add("active");
    btn.addEventListener("click", () => {
      if (fileBarOnPick) fileBarOnPick(f.path);
    });
    bar.appendChild(btn);
  });
}

async function loadFile(path) {
  const res = await fetch("/api/load", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ path }),
  }).then(r => r.json());
  if (!res.ok) throw new Error(res.error || "load failed");
  currentFile = res.path;
  return res;
}

function getMetric(row, metric) {
  if (row[metric] != null) return row[metric];
  if (row.extra && row.extra[metric] != null) return row.extra[metric];
  return null;
}

function isTimingMetric(m) {
  return timingMetrics.includes(m);
}

function formatMetric(v, metric) {
  if (v == null) return "–";
  if (isTimingMetric(metric)) return v.toFixed(1) + " ms";
  if (Math.abs(v) >= 1000 || Number.isInteger(v)) return v.toLocaleString();
  return v.toPrecision(4);
}

function syncMetricSelect(sel) {
  const prev = sel.value;
  sel.innerHTML = "";
  const og1 = document.createElement("optgroup");
  og1.label = "Timing";
  timingMetrics.forEach(m => og1.appendChild(new Option(m, m)));
  sel.appendChild(og1);
  if (extraMetrics.length) {
    const og2 = document.createElement("optgroup");
    og2.label = "Counters";
    extraMetrics.forEach(m => og2.appendChild(new Option(m, m)));
    sel.appendChild(og2);
  }
  sel.value = (prev && [...sel.options].some(o => o.value === prev)) ? prev : "total_median";
}

function syncSystemSelect(sel, preferred) {
  const prev = sel.value;
  sel.innerHTML = "";
  systems.forEach(s => sel.appendChild(new Option(s, s)));
  if (systems.includes(prev)) sel.value = prev;
  else if (preferred && systems.includes(preferred)) sel.value = preferred;
  else sel.value = systems[0];
}

async function init() {
  await fetchFiles();
  await fetchData();

  // ---------- Boxplot: one box per system, distribution of total_median across queries ----------
  function renderBox() {
    const boxTraces = systems.map(s => {
      const pts = data.filter(r => r.system === s && r.total_median != null);
      return {
        type: "box",
        name: s,
        y: pts.map(r => r.total_median),
        text: pts.map(r => r.query),
        hovertemplate: "%{text}: %{y:.1f} ms<extra>" + s + "</extra>",
        boxpoints: "outliers",
        marker: { color: sysColor[s] },
        line: { color: sysColor[s] },
      };
    });
    Plotly.react("boxplot", boxTraces, {
      margin: { t: 10, r: 10, b: 50, l: 60 },
      yaxis: { title: "total runtime (ms)", type: "log" },
      showlegend: false,
    }, { responsive: true, displaylogo: false });
  }

  // ---------- Bar plot: per query, grouped by system ----------
  const barMetricSel = document.getElementById("barMetric");
  syncMetricSelect(barMetricSel);
  function renderBars() {
    const metric = barMetricSel.value;
    const scale = document.getElementById("barScale").value;
    const timing = isTimingMetric(metric);
    const traces = systems.map(s => {
      const ptsBy = {};
      data.filter(r => r.system === s).forEach(r => { ptsBy[r.query] = r; });
      return {
        type: "bar",
        name: s,
        x: queries,
        y: queries.map(q => ptsBy[q] ? getMetric(ptsBy[q], metric) : null),
        marker: { color: sysColor[s] },
        customdata: queries.map(q => {
          const r = ptsBy[q];
          return r ? [formatMetric(getMetric(r, metric), metric)] : ["–"];
        }),
        hovertemplate:
          "<b>%{x}</b> · " + s +
          "<br>" + metric + ": %{customdata[0]}<extra></extra>",
      };
    });
    Plotly.react("bars", traces, {
      barmode: "group",
      margin: { t: 10, r: 10, b: 90, l: 60 },
      yaxis: { title: metric + (timing ? " (ms)" : ""), type: scale },
      xaxis: { tickangle: -60, automargin: true },
      legend: { orientation: "h", y: 1.1 },
    }, { responsive: true, displaylogo: false });
  }
  barMetricSel.onchange = renderBars;
  document.getElementById("barScale").onchange = renderBars;

  // ---------- A/B comparison ----------
  const sysASel = document.getElementById("sysA");
  const sysBSel = document.getElementById("sysB");
  const cmpMetricSel = document.getElementById("cmpMetric");
  syncSystemSelect(sysASel, systems[0]);
  syncSystemSelect(sysBSel, systems[1] || systems[0]);
  syncMetricSelect(cmpMetricSel);

  function indexBy(rows) {
    const m = {};
    rows.forEach(r => { m[r.query] = r; });
    return m;
  }

  function geoMean(xs) {
    const valid = xs.filter(x => x > 0 && isFinite(x));
    if (!valid.length) return null;
    const s = valid.reduce((acc, v) => acc + Math.log(v), 0);
    return Math.exp(s / valid.length);
  }

  function renderComparison() {
    const a = sysASel.value, b = sysBSel.value;
    const metric = cmpMetricSel.value;
    const timing = isTimingMetric(metric);
    const unit = timing ? " ms" : "";
    document.getElementById("lblA1").textContent = a;
    document.getElementById("lblB1").textContent = b;
    document.getElementById("lblA2").textContent = a;
    document.getElementById("lblB2").textContent = b;
    document.getElementById("lblMetric1").textContent = metric;

    const ra = indexBy(data.filter(r => r.system === a));
    const rb = indexBy(data.filter(r => r.system === b));
    const items = queries
      .filter(q => ra[q] && rb[q])
      .map(q => ({ q, ta: getMetric(ra[q], metric), tb: getMetric(rb[q], metric) }))
      .filter(i => i.ta != null && i.tb != null && i.ta > 0 && i.tb > 0)
      .map(i => ({ ...i, sp: i.tb / i.ta }));
    items.sort((x, y) => y.sp - x.sp);

    const aWins = items.filter(i => i.sp > 1).length;
    const bWins = items.filter(i => i.sp < 1).length;
    const gm = geoMean(items.map(i => i.sp));
    document.getElementById("cmpStats").innerHTML =
      `${items.length} shared queries · A smaller in ${aWins}, B smaller in ${bWins}` +
      (gm ? ` · geomean ratio ${gm.toFixed(2)}×` : "");

    // Sorted ratio bars
    const colors = items.map(i => i.sp >= 1 ? "#059669" : "#dc2626");
    const useLogY = items.length > 0 && (Math.max(...items.map(i => i.sp)) / Math.min(...items.map(i => i.sp)) > 50);
    Plotly.react("speedup", [{
      type: "bar",
      x: items.map((_, idx) => idx),
      y: items.map(i => i.sp),
      marker: { color: colors },
      customdata: items.map(i => [i.q, formatMetric(i.ta, metric), formatMetric(i.tb, metric), i.sp.toFixed(2)]),
      hovertemplate:
        "<b>%{customdata[0]}</b>" +
        "<br>" + a + ": %{customdata[1]}" +
        "<br>" + b + ": %{customdata[2]}" +
        "<br>B/A: %{customdata[3]}×<extra></extra>",
    }], {
      margin: { t: 10, r: 10, b: 30, l: 60 },
      yaxis: { title: "ratio B/A (×)" + (useLogY ? ", log" : ""), type: useLogY ? "log" : "linear" },
      xaxis: { title: "queries (sorted by ratio)", showticklabels: false },
      shapes: [{
        type: "line", x0: -0.5, x1: items.length - 0.5,
        y0: 1, y1: 1,
        line: { color: "#6b7280", dash: "dash", width: 1 },
      }],
      showlegend: false,
    }, { responsive: true, displaylogo: false });

    // A/B scatter
    const minV = items.length ? Math.min(...items.flatMap(i => [i.ta, i.tb])) : 1;
    const maxV = items.length ? Math.max(...items.flatMap(i => [i.ta, i.tb])) : 1;
    const useLogXY = items.length > 0 && (maxV / Math.max(minV, 1e-9) > 50);
    Plotly.react("abplot", [
      {
        type: "scatter",
        mode: "markers",
        x: items.map(i => i.ta),
        y: items.map(i => i.tb),
        marker: {
          size: 9,
          color: items.map(i => i.sp >= 1 ? "#059669" : "#dc2626"),
          opacity: 0.75,
          line: { width: 0.5, color: "#1a1d21" },
        },
        customdata: items.map(i => [i.q, formatMetric(i.ta, metric), formatMetric(i.tb, metric), i.sp.toFixed(2)]),
        hovertemplate:
          "<b>%{customdata[0]}</b>" +
          "<br>" + a + ": %{customdata[1]}" +
          "<br>" + b + ": %{customdata[2]}" +
          "<br>B/A: %{customdata[3]}×<extra></extra>",
      },
      {
        type: "scatter",
        mode: "lines",
        x: [minV, maxV],
        y: [minV, maxV],
        line: { color: "#6b7280", dash: "dash", width: 1 },
        hoverinfo: "skip",
        showlegend: false,
      },
    ], {
      margin: { t: 10, r: 10, b: 50, l: 60 },
      xaxis: { title: a + (unit ? " (" + unit.trim() + ")" : ""), type: useLogXY ? "log" : "linear" },
      yaxis: { title: b + (unit ? " (" + unit.trim() + ")" : ""), type: useLogXY ? "log" : "linear" },
      showlegend: false,
    }, { responsive: true, displaylogo: false });
  }

  sysASel.onchange = sysBSel.onchange = cmpMetricSel.onchange = renderComparison;

  function renderAll() {
    renderBox();
    renderBars();
    renderComparison();
  }
  renderAll();

  const fileBar = document.getElementById("fileBar");

  async function reloadAndRender(path) {
    const btns = [...fileBar.querySelectorAll("button")];
    btns.forEach(b => b.disabled = true);
    try {
      await loadFile(path);
      await fetchData();
      await fetchFiles();
      syncSystemSelect(sysASel);
      syncSystemSelect(sysBSel);
      syncMetricSelect(barMetricSel);
      syncMetricSelect(cmpMetricSel);
      renderAll();
    } catch (e) {
      alert("Load failed: " + e.message);
    } finally {
      [...fileBar.querySelectorAll("button")].forEach(b => b.disabled = false);
    }
  }

  fileBarOnPick = (path) => reloadAndRender(path);
}

init().catch(err => {
  document.body.insertAdjacentHTML("beforeend",
    "<pre style='color:#dc2626;'>" + err + "</pre>");
});
</script>
</body>
</html>
"""


def make_handler(state):
    """state holds 'base_dir', 'current_path', 'data_json' — mutated by /api/load."""
    def write(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index.html"):
                write(self, 200, "text/html; charset=utf-8", HTML.encode("utf-8"))
            elif self.path == "/api/data":
                write(self, 200, "application/json", state["data_json"].encode("utf-8"))
            elif self.path == "/api/files":
                body = json.dumps({
                    "files": list_csvs(state["base_dir"]),
                    "current": state["current_path"],
                    "base_dir": state["base_dir"],
                }).encode("utf-8")
                write(self, 200, "application/json", body)
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            if self.path == "/api/load":
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length else b""
                try:
                    req = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    req = {}
                target = req.get("path") or state["current_path"]
                safe = safe_path(state["base_dir"], target)
                if safe is None:
                    body = json.dumps({"ok": False, "error": f"invalid path: {target}"}).encode("utf-8")
                    write(self, 400, "application/json", body)
                    return
                try:
                    rows = load_results(safe)
                    state["current_path"] = safe
                    state["data_json"] = json.dumps(rows)
                    body = json.dumps({
                        "ok": True,
                        "path": safe,
                        "rows": len(rows),
                        "systems": len({r["system"] for r in rows}),
                        "queries": len({r["query"] for r in rows}),
                    }).encode("utf-8")
                    write(self, 200, "application/json", body)
                    sys.stderr.write(f"Loaded {safe}: {len(rows)} rows\n")
                except Exception as e:
                    body = json.dumps({"ok": False, "error": str(e)}).encode("utf-8")
                    write(self, 500, "application/json", body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    return Handler


class ReusingServer(socketserver.TCPServer):
    allow_reuse_address = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_path",
                        help="CSV result file or directory containing CSV result files")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    path = os.path.abspath(args.result_path)
    if os.path.isdir(path):
        base_dir = path
        files = list_csvs(base_dir)
        if not files:
            print(f"No CSV files found in {base_dir}", file=sys.stderr)
            sys.exit(1)
        current_path = files[0]["path"]
    elif os.path.isfile(path):
        base_dir = os.path.dirname(path) or os.getcwd()
        current_path = path
    else:
        print(f"Not a file or directory: {path}", file=sys.stderr)
        sys.exit(1)

    rows = load_results(current_path)
    systems = sorted({r["system"] for r in rows})
    queries = sorted({r["query"] for r in rows}, key=query_sort_key)
    print(f"Loaded {current_path}: {len(rows)} rows · {len(systems)} systems · {len(queries)} queries",
          file=sys.stderr)

    state = {
        "base_dir": base_dir,
        "current_path": current_path,
        "data_json": json.dumps(rows),
    }
    handler = make_handler(state)
    with ReusingServer((args.host, args.port), handler) as httpd:
        print(f"Serving on http://{args.host}:{args.port}", file=sys.stderr)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.", file=sys.stderr)


if __name__ == "__main__":
    main()
