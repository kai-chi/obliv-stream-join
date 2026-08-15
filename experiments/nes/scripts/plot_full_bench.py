#!/usr/bin/env python3
"""Nexmark Q8 (large, quarter-sampled) — join algorithm throughput comparison."""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# config, time_s, tuples_per_s (input-based)
results = [
    ("FK-MERG-L3", 29.286, 680_079),
    ("FK-SORT-L3", 33.979, 586_155),
    ("FK-MERG-L4", 33.645, 591_976),
    ("FK-SORT-L4", 37.893, 525_609),
    ("Hash Join", 52.728, 377_732),
    ("NFK-JOIN-L3", 88.931, 223_962),
    ("Nested-Loop Join", 239.128, 83_290),
]

FAMILY = {
    "FK-MERG-L3": "MERG", "FK-MERG-L4": "MERG",
    "FK-SORT-L3": "SORT", "FK-SORT-L4": "SORT",
    "NFK-JOIN-L3": "NFK",
    "Hash Join": "BASE", "Nested-Loop Join": "BASE",
}
# validated categorical palette (light mode); baselines in de-emphasis gray
COLOR = {"MERG": "#2a78d6", "SORT": "#eb6834", "NFK": "#1baf7a", "BASE": "#b9b8b1"}
TEXT_PRIMARY, TEXT_SECONDARY, SURFACE = "#0b0b0b", "#52514e", "#fcfcfb"

results = sorted(results, key=lambda r: r[2])  # ascending -> fastest ends up on top row
names = [r[0] for r in results]
tput = [r[2] / 1000 for r in results]  # kTup/s
colors = [COLOR[FAMILY[n]] for n in names]

fig, ax = plt.subplots(figsize=(8.6, 4.4), dpi=200)
fig.patch.set_facecolor(SURFACE)
ax.set_facecolor(SURFACE)

bars = ax.barh(names, tput, height=0.58, color=colors, zorder=3)

# direct value labels at the bar ends (relief for the contrast WARN, replaces a grid)
for bar, value, (_, secs, _) in zip(bars, tput, results):
    ax.annotate(
        f"{value:,.0f} kTup/s",
        xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
        xytext=(6, 0), textcoords="offset points",
        va="center", ha="left", fontsize=9, color=TEXT_PRIMARY)

# recessive axes: no grid (values are labeled), thin baseline only
for spine in ("top", "right", "bottom"):
    ax.spines[spine].set_visible(False)
ax.spines["left"].set_color("#d8d7d0")
ax.tick_params(axis="y", length=0, labelsize=10, labelcolor=TEXT_PRIMARY)
ax.set_xticks([])
ax.set_xlim(0, max(tput) * 1.22)

ax.set_title(
    "Nexmark Q8: input throughput by join algorithm",
    loc="left", fontsize=13, fontweight="bold", color=TEXT_PRIMARY, pad=18)
ax.text(
    0, 1.035, "Full large dataset (19.9M input tuples) · tumbling 10 s windows · "
    "1 worker thread · Release build",
    transform=ax.transAxes, fontsize=9, color=TEXT_SECONDARY)

legend_items = [
    mpatches.Patch(color=COLOR["MERG"], label="FK-MERG (oblivious, sorted windows)"),
    mpatches.Patch(color=COLOR["SORT"], label="FK-SORT (oblivious, full sort/join)"),
    mpatches.Patch(color=COLOR["NFK"], label="NFK-JOIN (oblivious, generic)"),
    mpatches.Patch(color=COLOR["BASE"], label="Insecure baselines"),
]
ax.legend(handles=legend_items, loc="lower right", fontsize=8.5, frameon=False,
          labelcolor=TEXT_SECONDARY)

fig.text(0.005, -0.04,
         "L4 variants emit worst-case padded output (19.9M tuples incl. dummies); "
         "L3 variants and baselines emit the 9,149,189 true matches.\n"
         "Throughput is input-based, so all bars are directly comparable. "
         "L3/baseline runs match the canonical full-dataset checksum.",
         fontsize=8, color=TEXT_SECONDARY, va="top")

fig.tight_layout()
out = __file__.replace(".py", ".png")
fig.savefig(out, bbox_inches="tight", facecolor=SURFACE)
print(out)
