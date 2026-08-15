#!/usr/bin/env python3
"""Density-scaling analysis: throughput curves + fitted scaling exponents."""
import csv
import math
import sys
from collections import defaultdict

import matplotlib.pyplot as plt

CSV = sys.argv[1] if len(sys.argv) > 1 else "scaling_results.csv"

LABEL = {
    "FK_MERG_L3": "FK-MERG-L3", "FK_MERG_L4": "FK-MERG-L4",
    "FK_SORT_L3": "FK-SORT-L3", "FK_SORT_L4": "FK-SORT-L4",
    "NFK_JOIN_L3": "NFK-JOIN-L3", "HASH_JOIN": "Hash Join",
    "NESTED_LOOP_JOIN": "Nested-Loop Join",
}
COLOR = {"FK_MERG": "#2a78d6", "FK_SORT": "#eb6834", "NFK_JOIN": "#1baf7a",
         "HASH_JOIN": "#b9b8b1", "NESTED_LOOP_JOIN": "#b9b8b1"}
TEXT_PRIMARY, TEXT_SECONDARY, SURFACE = "#0b0b0b", "#52514e", "#fcfcfb"

def family(alg):
    for prefix in ("FK_MERG", "FK_SORT", "NFK_JOIN"):
        if alg.startswith(prefix):
            return prefix
    return alg

def style(alg):
    dashed = alg.endswith("_L4") or alg == "NESTED_LOOP_JOIN"
    return {"color": COLOR[family(alg)], "linestyle": "--" if dashed else "-"}

data = defaultdict(dict)  # alg -> pct -> (time, tput, count)
with open(CSV) as f:
    for row in csv.reader(f):
        if len(row) >= 4 and row[2] != "FAILED":
            data[row[0]][int(row[1])] = (float(row[2]), float(row[3]), int(row[4]) if len(row) > 4 and row[4] else 0)

def dodge_labels(ax, labels, min_gap_frac=0.045, log=False):
    """labels: list of (y, text, color). Spread vertically so none overlap."""
    lo, hi = ax.get_ylim()
    span = (math.log(hi) - math.log(lo)) if log else (hi - lo)
    gap = span * min_gap_frac
    items = sorted(labels, key=lambda t: (math.log(t[0]) if log else t[0]))
    placed = []
    for y, text, color in items:
        yv = math.log(y) if log else y
        if placed and yv - placed[-1] < gap:
            yv = placed[-1] + gap
        placed.append(yv)
        y_out = math.exp(yv) if log else yv
        xlim = ax.get_xlim()
        x_end = xlim[1] if not log else math.exp(math.log(xlim[1]) * 0.985)
        ax.annotate(text, xy=(100, y), xytext=(x_end, y_out),
                    va="center", ha="left" if not log else "left",
                    fontsize=8, color=color,
                    annotation_clip=False)

order = ["FK_MERG_L3", "FK_MERG_L4", "FK_SORT_L3", "FK_SORT_L4", "NFK_JOIN_L3", "HASH_JOIN", "NESTED_LOOP_JOIN"]
order = [a for a in order if a in data]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.2, 4.8), dpi=200)
fig.patch.set_facecolor(SURFACE)

# --- Panel 1: throughput vs data fraction ---
ax1.set_facecolor(SURFACE)
p1_labels = []
for alg in order:
    pts = sorted(data[alg].items())
    xs = [p for p, _ in pts]
    ys = [v[1] / 1000 for _, v in pts]
    ax1.plot(xs, ys, marker="o", markersize=3.5, linewidth=2, zorder=3, **style(alg))
    p1_labels.append((ys[-1], LABEL[alg], TEXT_PRIMARY))
ax1.set_title("Input throughput vs data density", loc="left", fontsize=12,
              fontweight="bold", color=TEXT_PRIMARY)
ax1.set_xlabel("data fraction (%)", fontsize=9, color=TEXT_SECONDARY)
ax1.set_ylabel("kTup/s (input)", fontsize=9, color=TEXT_SECONDARY)
ax1.set_xlim(5, 103)
ax1.set_xticks(range(10, 101, 10))
dodge_labels(ax1, p1_labels)

# --- Panel 2: log-log time vs fraction with fitted exponents ---
ax2.set_facecolor(SURFACE)

def fit_slope(xs, ts):
    lx = [math.log(x) for x in xs]
    ly = [math.log(t) for t in ts]
    n = len(lx)
    mx, my = sum(lx) / n, sum(ly) / n
    return sum((a - mx) * (b - my) for a, b in zip(lx, ly)) / sum((a - mx) ** 2 for a in lx)

exponents, upper_exponents = {}, {}
p2_labels = []
for alg in order:
    pts = sorted(data[alg].items())
    xs = [p for p, _ in pts]
    ts = [v[0] for _, v in pts]
    exponents[alg] = fit_slope(xs, ts)
    upper = [(x, t) for x, t in zip(xs, ts) if x >= 50]
    upper_exponents[alg] = fit_slope([x for x, _ in upper], [t for _, t in upper])
    ax2.plot(xs, ts, marker="o", markersize=3.5, linewidth=2, zorder=3, **style(alg))
    p2_labels.append((ts[-1], f"{LABEL[alg]}  (^{upper_exponents[alg]:.2f})", TEXT_PRIMARY))
ax2.set_xscale("log")
ax2.set_yscale("log")
ax2.set_title("Runtime vs density (log-log; exponent fitted on 50-100%)", loc="left",
              fontsize=12, fontweight="bold", color=TEXT_PRIMARY)
ax2.set_xlabel("data fraction (%)", fontsize=9, color=TEXT_SECONDARY)
ax2.set_ylabel("time (s)", fontsize=9, color=TEXT_SECONDARY)
ax2.set_xlim(9, 300)
dodge_labels(ax2, p2_labels, log=True)

for ax in (ax1, ax2):
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#d8d7d0")
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=8)
    ax.grid(axis="y", color="#eceae4", linewidth=0.7, zorder=0)

fig.suptitle("Nexmark Q8, density-scaling sweep — 10%..100% per-tuple samples of the large dataset "
             "(19.9M tuples at 100%) · 1 worker thread · Release · GCP TDX VM",
             x=0.01, ha="left", fontsize=9.5, color=TEXT_SECONDARY, y=1.0)
fig.tight_layout(rect=(0, 0, 1, 0.96))
out = CSV.replace(".csv", ".png")
fig.savefig(out, bbox_inches="tight", facecolor=SURFACE)
print(out)
print("\nFitted exponents (time ~ density^k):  all-points | 50-100% only")
for alg in order:
    print(f"  {LABEL[alg]:<18} {exponents[alg]:.2f} | {upper_exponents[alg]:.2f}")
