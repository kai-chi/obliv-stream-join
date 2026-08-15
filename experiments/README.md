# experiments/

These scripts drive `../app` over a range of parameters and turn the output into the plots in the paper. 

Run everything from inside this directory (the scripts use relative paths like `results/...` and invoke the app via `cwd='../'`), and build `../app` at least once first (see the top-level README).

## config.yaml

Every script reads this before doing anything:

```yaml
compile:     0   # 1 = `make clean && make` before running (see compile_app() in commons.py)
experiment:  0   # 1 = actually run ./app and collect fresh data into results/*.csv
repetitions: 1   # take the median of this many runs per data point
plot:        1   # 1 = (re)generate the PNG from whatever is in results/*.csv
```

It's shared and global, not per-script — if you flip `experiment` on, every script you run afterwards will try to collect fresh data instead of just plotting. The repo already ships with `results/*.csv` from a prior run, so with the defaults above (`experiment: 0`, `plot: 1`) any script just redraws its plot from existing data — that's the fastest way to check the plotting code without waiting on the app.

`compile: 1` is separate from `experiment` on purpose — most of the time you build once, then run several experiment scripts against the same binary.

## What each script does

| Script | Sweeps | Writes | Roughly reproduces |
|---|---|---|---|
| `general-performance.py` | fixed configs (`synth-1`, `synth-2`, `tpch-1`) across every algorithm | `results/general-performance.csv` | Fig. 5 (throughput per algorithm/leakage level) |
| `window-size.py` | window size, log-spaced 128 .. 2^20 | `results/window-size.csv` | Fig. 6a-c / 7a (throughput vs. window size, per leakage level) |
| `batch-size.py` | batch size, log-spaced 100 .. 50000 | `results/batch-size.csv` | Fig. 6d-f / 7b (throughput vs. batch size) |
| `window-batch-size.py` | window size (batch pinned to log2(window)) for `FK-MERG-L3` vs `FK-SORT-L3` | `results/window-batch-size.csv` | the MERG-vs-SORT speedup comparison |
| `plot-fk.py`, `plot-nfk.py` | — (plot-only) | reads `window-size.csv` + `batch-size.csv` | Figs. 6 and 7, combined multi-panel versions |
| `plot-figure-triplets.py` | — (plot-only) | reads `window-size.csv` + `batch-size.csv`| the per-leakage-level combined panels (`l2-all-plots.png` etc.) and the improvement-factor plots; a grab-bag of plotting functions, only one is actually called in `__main__` at a time — comment/uncomment as needed |

`window-size.py`, `batch-size.py`, and `window-batch-size.py` both collect data *and* draw their own simple plot at the bottom of the file. `plot-fk.py`, `plot-nfk.py`, and `plot-figure-triplets.py` are plot-only — they never touch `./app`, they just read the CSVs the other scripts already produced. Run those after, not instead of, the data-collecting scripts.

To run one experiment end to end:

```bash
cd experiments
# edit config.yaml: experiment: 1
./window-size.py
```

Each of the sweep scripts hardcodes its own `algorithms = [...]` list near the bottom (under `if __name__ == '__main__':`) — the list actually used is whichever line isn't commented out. Edit it to add/remove algorithms before running; there's no CLI flag for this.

