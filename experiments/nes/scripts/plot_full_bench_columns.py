#!/usr/bin/env python3
"""Full-dataset Nexmark Q8 throughput, grouped by leakage profile (Fig 5 style).

Style follows experiments/general-performance.py + commons.py: leakage bands
as alpha-0.2 axvspans (L2 #E5E4E2, L3 #848884, L4 #8A9A5B), dashed dividers
between the leakage groups, math-text leakage labels on top, log y-axis,
black bar edges, categorical colors from color_categorical. FK and non-FK
algorithms share one axis, grouped purely by leakage level.
"""
import matplotlib.pyplot as plt

# (leakage, [(alg, throughput K rec/s, color)]) — one flat group per level.
# Insert the pending L2 entries once measured.
GROUPS = [
    ("$\\mathcal{L}_2$", [
        ("FK-MERG-L2", 52.761, "#cb5d00"),
        ("FK-SORT-L2", 33.065, "#b44b20"),
        ("NFK-JOIN-L2", 2.981, "#e6ab48"),
    ]),
    ("$\\mathcal{L}_3$", [
        ("FK-MERG-L3", 680.079, "#147af3"),
        ("FK-SORT-L3", 486.155, "#7b8b3d"),
        ("NFK-JOIN-L3", 223.962, "#885a20"),
    ]),
    ("$\\mathcal{L}_4$", [
        ("FK-MERG-L4", 591.976, "#7326d3"),
        ("FK-SORT-L4", 425.609, "#c7b186"),
        ("NLJ-L4", 38.925, "#bce931"),
    ]),
]
BAND = {"$\\mathcal{L}_2$": "#E5E4E2", "$\\mathcal{L}_3$": "#848884", "$\\mathcal{L}_4$": "#8A9A5B"}

plt.rcParams.update(plt.rcParamsDefault)
plt.figure(figsize=(4.6, 3.2))
ax = plt.gca()

names, values, colors = [], [], []
band_edges = []  # (start, end, band_color, label)
pos = 0
for label, entries in GROUPS:
    if not entries:
        continue
    start = pos - 0.5
    for alg, value, color in entries:
        names.append(alg)
        values.append(value)
        colors.append(color)
        pos += 1
    band_edges.append((start, pos - 0.5, BAND[label], label))

for start, end, color, _ in band_edges:
    ax.axvspan(start, end, facecolor=color, alpha=0.2)
for (start, _, _, _) in band_edges[1:]:
    ax.axvline(x=start, linestyle="--", color="black", linewidth=0.75)

ax.bar(range(len(names)), values, color=colors, width=0.65, edgecolor="black", linewidth=0.8, zorder=3)
ax.set_yscale("log")
ax.set_ylim(1, 4000)
ax.set_yticks([1, 10, 100, 1000])
ax.set_ylabel("Throughput [K rec / s]")
ax.set_xticks(range(len(names)))
ax.set_xticklabels(names, rotation=30, ha="right", fontsize=10)
ax.set_xlim(-0.5, len(names) - 0.5)

for start, end, _, label in band_edges:
    ax.text((start + end) / 2, 2000, label, fontsize=15, ha="center")

plt.rcParams.update({"font.size": 15})
plt.tight_layout()
out = __file__.rsplit("/", 1)[0] + "/../figures/bench_full.png"
plt.savefig(out, transparent=False, bbox_inches="tight", pad_inches=0.1, dpi=200)
print("Saved image file: " + out)
