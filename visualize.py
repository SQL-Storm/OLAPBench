#!/usr/bin/env python3
"""Serve a webpage that visualizes benchmark results from a CSV file."""

import argparse
import csv
import http.server
import io
import json
import os
import posixpath
import re
import shlex
import socketserver
import subprocess
import sys
from ast import literal_eval


def _raise_csv_field_limit():
    """Some result fields (query text, extra blobs) exceed csv's default 128K cap."""
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 2


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
        v = float(s)
    except ValueError:
        return None
    return v if v == v else None  # filter NaN


def query_sort_key(q):
    m = re.match(r"(\d+)([a-zA-Z]*)", q)
    if m:
        return (int(m.group(1)), m.group(2), q)
    return (10**9, "", q)


_HOST_RE = re.compile(r"^([A-Za-z0-9_][A-Za-z0-9._-]*@)?[A-Za-z0-9_][A-Za-z0-9._-]*$")


def parse_remote(spec):
    """Detect scp-style 'user@host:path', 'host:path', or 'alias:path'. Returns (host, path) or None."""
    if ":" not in spec:
        return None
    left, _, right = spec.partition(":")
    if not _HOST_RE.match(left):
        return None
    return left, right or "."


def load_results(stream):
    rows = []
    _raise_csv_field_limit()
    reader = csv.DictReader(stream)
    for row in reader:
        state = row.get("state") or ""
        success = state == "success"
        extra_numeric = {}
        if success:
            extra = parse_extra(row.get("extra", ""))
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if isinstance(v, bool):
                        continue
                    if isinstance(v, (int, float)) and v == v:  # filter NaN
                        extra_numeric[k] = v
        total = parse_list(row.get("total", "")) if success else []
        total_mean = parse_float(row.get("total_mean")) if success else None
        total_median = parse_float(row.get("total_median")) if success else None
        client_total_median = parse_float(row.get("client_total_median")) if success else None
        # Embedded systems (e.g. DuckDB) report no server-side total; fall back to client timing.
        if success and total_median is None:
            total = parse_list(row.get("client_total", ""))
            total_mean = parse_float(row.get("client_total_mean"))
            total_median = client_total_median
        rows.append({
            "system": row["title"],
            "query": row["query"],
            "state": state,
            "total": total,
            "total_mean": total_mean,
            "total_median": total_median,
            "execution_median": parse_float(row.get("execution_median")) if success else None,
            "compilation_median": parse_float(row.get("compilation_median")) if success else None,
            "client_total_median": client_total_median,
            "extra": extra_numeric,
        })
    return rows


class LocalSource:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)

    def label(self):
        return self.base_dir

    def host_prefix(self):
        return ""

    def list(self):
        entries = []
        try:
            names = os.listdir(self.base_dir)
        except OSError:
            return entries
        for name in names:
            if not name.endswith(".csv"):
                continue
            full = os.path.join(self.base_dir, name)
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

    def validate(self, candidate):
        if not candidate:
            return None
        full = os.path.abspath(candidate)
        if full != self.base_dir and not full.startswith(self.base_dir + os.sep):
            return None
        if not full.endswith(".csv"):
            return None
        if not os.path.isfile(full):
            return None
        return full

    def load(self, path):
        with open(path, newline="") as fh:
            return load_results(fh)


class RemoteSource:
    def __init__(self, host, base_dir):
        self.host = host
        self.base_dir = base_dir.rstrip("/") or "."

    def label(self):
        return f"{self.host}:{self.base_dir}"

    def host_prefix(self):
        return f"{self.host}:"

    def _run(self, command):
        return subprocess.run(
            ["ssh", "-o", "BatchMode=yes", self.host, command],
            check=True, capture_output=True, text=True,
        )

    def list(self):
        cmd = (
            f"find {shlex.quote(self.base_dir)} -maxdepth 1 -type f "
            f"-name '*.csv' -printf '%T@\\t%p\\n'"
        )
        try:
            res = self._run(cmd)
        except subprocess.CalledProcessError as e:
            sys.stderr.write(f"ssh list failed: {e.stderr.strip()}\n")
            return []
        entries = []
        for line in res.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                mtime_str, full = line.split("\t", 1)
                mtime = float(mtime_str)
            except ValueError:
                continue
            entries.append({
                "name": posixpath.basename(full),
                "path": full,
                "mtime": mtime,
            })
        entries.sort(key=lambda e: -e["mtime"])
        return entries

    def validate(self, candidate):
        if not candidate or not candidate.endswith(".csv"):
            return None
        norm = posixpath.normpath(candidate)
        base = posixpath.normpath(self.base_dir)
        if norm != base and not norm.startswith(base + "/"):
            return None
        return norm

    def load(self, path):
        res = self._run(f"cat {shlex.quote(path)}")
        return load_results(io.StringIO(res.stdout))


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
  table.summary { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
  table.summary th, table.summary td {
    padding: 0.4rem 0.6rem;
    text-align: right;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  table.summary th { font-weight: 600; color: var(--muted); }
  table.summary th:first-child, table.summary td:first-child { text-align: left; }
  table.summary tr:last-child td { border-bottom: none; }
  table.summary td.num { font-variant-numeric: tabular-nums; }
  .sys-swatch {
    display: inline-block;
    width: 0.7rem;
    height: 0.7rem;
    border-radius: 2px;
    margin-right: 0.4rem;
    vertical-align: -1px;
  }
</style>
</head>
<body>
  <div class="header">
    <h1>Benchmark Results</h1>
    <div class="meta" id="meta"></div>
  </div>
  <div class="file-bar" id="fileBar"></div>

  <div class="row">
    <div class="card">
      <h2>Summary per system</h2>
      <div id="summary"></div>
    </div>
    <div class="card">
      <h2>Failing queries</h2>
      <div id="failures"></div>
    </div>
  </div>

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
let currentFile = null, baseDir = "", hostPrefix = "";
let timingMetrics = ["total_median", "total_mean", "execution_median", "compilation_median", "client_total_median"];
let extraMetrics = [];

function basename(p) {
  if (!p) return "";
  const i = Math.max(p.lastIndexOf("/"), p.lastIndexOf("\\"));
  return i >= 0 ? p.slice(i + 1) : p;
}

function updateMeta() {
  const full = currentFile ? hostPrefix + currentFile : "";
  const successCount = data.filter(r => r.state === "success").length;
  document.getElementById("meta").textContent =
    `${full ? full + " · " : ""}${data.length} runs (${successCount} successful) · ${systems.length} systems · ${queries.length} queries`;
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
  hostPrefix = res.host_prefix || "";
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

  // ---------- Summary table: counts and runtime aggregates per system ----------
  function median(xs) {
    if (!xs.length) return null;
    const sorted = [...xs].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }
  function fmtMs(v) {
    if (v == null) return "–";
    return v.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + " ms";
  }
  function renderSummary() {
    const rowsBySys = systems.map(s => {
      const all = data.filter(r => r.system === s);
      const succ = all.filter(r => r.state === "success");
      const times = succ.map(r => r.total_median).filter(v => v != null);
      const total = times.reduce((a, b) => a + b, 0);
      return {
        system: s,
        success: succ.length,
        fatal: all.filter(r => r.state === "fatal").length,
        error: all.filter(r => r.state === "error").length,
        timeout: all.filter(r => r.state === "timeout" || r.state === "global_timeout").length,
        total: times.length ? total : null,
        avg: times.length ? total / times.length : null,
        med: median(times),
      };
    });
    let html = `<table class="summary"><thead><tr>
      <th>System</th>
      <th>Success</th><th>Fatal</th><th>Error</th><th>Timeout</th>
      <th>Total runtime</th><th>Avg runtime</th><th>Median runtime</th>
    </tr></thead><tbody>`;
    rowsBySys.forEach(c => {
      html += `<tr>
        <td><span class="sys-swatch" style="background:${sysColor[c.system]}"></span>${c.system}</td>
        <td class="num">${c.success}</td>
        <td class="num">${c.fatal}</td>
        <td class="num">${c.error}</td>
        <td class="num">${c.timeout}</td>
        <td class="num">${fmtMs(c.total)}</td>
        <td class="num">${fmtMs(c.avg)}</td>
        <td class="num">${fmtMs(c.med)}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
    document.getElementById("summary").innerHTML = html;
  }

  // ---------- Failing queries table: grouped by state (fatal → error → timeout) ----------
  function renderFailures() {
    const groups = [
      { states: ["fatal"], label: "Fatal" },
      { states: ["error"], label: "Error" },
      { states: ["timeout", "global_timeout"], label: "Timeout" },
    ];
    const out = [];
    groups.forEach(g => {
      const stateSet = new Set(g.states);
      const byQuery = {};
      data.forEach(r => {
        if (stateSet.has(r.state)) {
          (byQuery[r.query] = byQuery[r.query] || []).push(r.system);
        }
      });
      Object.entries(byQuery)
        .map(([q, sys]) => ({ query: q, systems: [...sys].sort() }))
        .sort((a, b) => b.systems.length - a.systems.length || compareQ(a.query, b.query))
        .forEach(e => out.push({ type: g.label, ...e }));
    });

    let html = `<table class="summary"><thead><tr>
      <th>Type</th><th>Query</th><th>Systems</th>
    </tr></thead><tbody>`;
    if (!out.length) {
      html += `<tr><td colspan="3" style="text-align:center; color: var(--muted)">No failures</td></tr>`;
    }
    out.forEach(r => {
      const sysHtml = r.systems.map(s =>
        `<span style="white-space:nowrap; margin-right:0.5rem;"><span class="sys-swatch" style="background:${sysColor[s] || "#999"}"></span>${s}</span>`
      ).join("");
      html += `<tr>
        <td>${r.type}</td>
        <td>${r.query}</td>
        <td style="text-align:left">${sysHtml}</td>
      </tr>`;
    });
    html += `</tbody></table>`;
    document.getElementById("failures").innerHTML = html;
  }

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
    renderSummary();
    renderFailures();
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
    """state holds 'source', 'current_path', 'data_json' — mutated by /api/load."""
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
                source = state["source"]
                body = json.dumps({
                    "files": source.list(),
                    "current": state["current_path"],
                    "base_dir": source.label(),
                    "host_prefix": source.host_prefix(),
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
                source = state["source"]
                safe = source.validate(target)
                if safe is None:
                    body = json.dumps({"ok": False, "error": f"invalid path: {target}"}).encode("utf-8")
                    write(self, 400, "application/json", body)
                    return
                try:
                    rows = source.load(safe)
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


def resolve_source(result_path):
    """Return (source, current_path). Exits on error."""
    remote = parse_remote(result_path)
    if remote:
        host, remote_path = remote
        check = (
            f"if [ -d {shlex.quote(remote_path)} ]; then echo dir; "
            f"elif [ -f {shlex.quote(remote_path)} ]; then echo file; "
            f"else echo none; fi"
        )
        try:
            res = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", host, check],
                check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"ssh failed: {e.stderr.strip() or e}", file=sys.stderr)
            sys.exit(1)
        kind = res.stdout.strip()
        if kind == "dir":
            source = RemoteSource(host, remote_path)
            files = source.list()
            if not files:
                print(f"No CSV files found in {source.label()}", file=sys.stderr)
                sys.exit(1)
            return source, files[0]["path"]
        if kind == "file":
            base_dir = posixpath.dirname(remote_path) or "."
            return RemoteSource(host, base_dir), remote_path
        print(f"Not a file or directory: {host}:{remote_path}", file=sys.stderr)
        sys.exit(1)

    path = os.path.abspath(result_path)
    if os.path.isdir(path):
        source = LocalSource(path)
        files = source.list()
        if not files:
            print(f"No CSV files found in {source.label()}", file=sys.stderr)
            sys.exit(1)
        return source, files[0]["path"]
    if os.path.isfile(path):
        return LocalSource(os.path.dirname(path) or os.getcwd()), path
    print(f"Not a file or directory: {path}", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_path",
                        help="CSV result file or directory containing CSV result files. "
                             "Use 'user@host:/path' to open a remote location over SSH.")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    source, current_path = resolve_source(args.result_path)

    rows = source.load(current_path)
    systems = sorted({r["system"] for r in rows})
    queries = sorted({r["query"] for r in rows}, key=query_sort_key)
    print(f"Loaded {current_path}: {len(rows)} rows · {len(systems)} systems · {len(queries)} queries",
          file=sys.stderr)

    state = {
        "source": source,
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
