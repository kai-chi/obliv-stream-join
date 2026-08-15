#!/usr/bin/python3
"""
Thread-scalability sweep for the non-ORAM FK stream joins.

Answers "do these joins actually use the cores we give them?" for the four
algorithms that are worth asking about - FK-MERG-L3/L4 (OAppend) and
FK-SORT-L3/L4 (Opaque-style full re-sort). The ORAM family and the L2
(tuple-at-a-time) variants are deliberately not here: the former is not
parallelised at all, the latter forces batch size 1, so neither has a thread
axis to sweep.

Setup, fixed across the sweep:

    window        2^22 tuples per stream (--window to change)
    batch         16384 tuples per stream (--batch to change)
    threads       1, 2, 4, 8, 16, 20, 32 (20 = one socket's cores)
    input         window + BATCHES*batch tuples per stream

Why BATCHES can be small: the app prefills both windows to their full size
*before* starting the timer, and retire() holds them at exactly window_size
afterwards. So every measured batch does identical work and steady state starts
at batch 1 - unlike large-window.py, this sweep does not need input = 4*window
to get a meaningful number. 12 batches is enough to average out noise.

Why batch 16384: OAppend pads the incoming batch up to the *window's* size
before merging, and FK-SORT re-sorts the whole window either way, so per-batch
cost is set by the window and is very nearly independent of the batch size. A
4x bigger batch therefore costs the same wall clock per batch while carrying 4x
the tuples - it raises reported throughput ~4x and leaves the shape of the
speedup curve alone. Runtime for a given batch count is unchanged. Pass
--batch 65536 for numbers directly comparable to large-window.py.

Throughput is computed here, not scraped: it is

    (r_size + s_size - r_window - s_window) / joinTotalTime   [M rec/s]

i.e. tuples that actually streamed through the join, divided by the measured
join time. The app prints its own joinThroughput line, but the two families did
not agree on whether the window prefill counts, so deriving it here keeps
FK-MERG and FK-SORT on the same axis regardless of app version.

NUMA: this box has 20 cores (40 logical) per socket, and the enclave's EPC
pages sit on whichever node first touched them. A 1->32 sweep left to the
scheduler therefore crosses sockets partway up and the curve bends down.
--pin-node 0 confines the whole sweep to one socket (32 <= 40 logical CPUs), so
run both if you want to separate "the algorithm stopped scaling" from "the
second socket is remote memory".

The sweep is restartable: results are appended and a config already present in
the CSV is skipped.

Usage:

    ./thread-scalability.py --dry-run          # plan only, run nothing
    ./thread-scalability.py                    # honours config.yaml
    ./thread-scalability.py --pin-node 0       # socket-local series
    ./thread-scalability.py --threads 1 2 4 8  # shorter sweep
"""
import argparse
import os
import signal
import statistics
import subprocess
import sys

import pandas as pd
from matplotlib import pyplot as plt

from commons import *

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'

# throughput/latency are derived here (see module docstring); matches is the
# app's own "Join matches" count, kept so a thread count that changes the
# answer is visible in the CSV instead of having to be re-run to find out.
HEADER = ('algorithm,window,batch,threads,pin,input-tuples,time,throughput,'
          'latency,matches,status\n')

ALGORITHMS = ['FK-MERG-L4', 'FK-MERG-L3', 'FK-SORT-L4', 'FK-SORT-L3']
WINDOW = 2 ** 22
BATCH = 16384
BATCHES = 12
THREADS = [1, 2, 4, 8, 16, 20, 32]  # 20 = cores per socket, so the socket boundary is visible
RATE = 1000  # tuples/s, same as the previous scalability figure

DEFAULT_TIMEOUT_S = 60 * 60

# Physical cores per socket on the SGX box (2 x Xeon Silver 4416+, 40 logical
# CPUs per socket once SMT is counted). Past this many threads the two series
# diverge for different reasons, which is what the marker in the plots explains:
# an unpinned run starts placing threads on the second socket, where the
# enclave's EPC pages are remote; a --pin-node run stays local and starts
# doubling up on SMT siblings instead.
SOCKET_CORES = 20


def build_config(algorithm, window, batch, batches, threads):
    """One sweep point. Everything is explicit so the only variable between
    rows of a series is --nthreads."""
    cfg = Config(algorithm=algorithm)
    cfg.fk_join = True
    cfg.r_window = window
    cfg.s_window = window
    cfg.r_batch = batch
    cfg.s_batch = batch
    cfg.r_size = window + batches * batch
    cfg.s_size = window + batches * batch
    cfg.r_rate = RATE
    cfg.s_rate = RATE
    cfg.nthreads = threads
    return cfg


def streamed_tuples(cfg):
    """Tuples that pass through the join, i.e. everything past the window fill.
    This is the numerator of the throughput reported here."""
    return cfg.r_size + cfg.s_size - cfg.r_window - cfg.s_window


def append_row(row):
    with open(res_file, 'a') as f:
        f.write(row + '\n')


def ensure_header():
    """Create the CSV, or prepend the header if the first line is data (an
    empty file left by `touch` would otherwise become the column names)."""
    if not os.path.exists(res_file) or os.path.getsize(res_file) == 0:
        init_file(res_file, HEADER)
        return
    with open(res_file) as f:
        lines = f.readlines()
    if lines[0].strip() == HEADER.strip():
        return
    print('Repairing ' + res_file + ': prepending the missing header')
    with open(res_file, 'w') as f:
        f.write(HEADER)
        f.writelines(lines)


def load_results():
    if not os.path.exists(res_file):
        return None
    try:
        data = pd.read_csv(res_file)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None
    return data if len(data) else None


def done_configs():
    """Points already measured, so a restarted sweep skips them. Failures and
    timeouts count as done."""
    data = load_results()
    if data is None:
        return set()
    return {(r['algorithm'], int(r['window']), int(r['batch']),
             int(r['threads']), str(r['pin'])) for _, r in data.iterrows()}


def failure_row(cfg, pin, status):
    return ','.join([cfg.algorithm, str(cfg.r_window), str(cfg.r_batch),
                     str(cfg.nthreads), pin, str(streamed_tuples(cfg)),
                     '-1', '-1', '-1', '-1', status])


def join(cfg, pin, repetitions, timeout_s):
    """Run one config `repetitions` times and append the median to the CSV.
    Returns 'ok', 'timeout' or 'error'; never exits, so one bad point does not
    abandon the sweep."""
    command = cfg.command()
    if pin != 'none':
        # Confine both the threads and their memory to one socket. The enclave
        # heap is first-touched by this process, so --membind decides where the
        # EPC pages the workers hammer actually live.
        command = ('numactl --cpunodebind=%s --membind=%s ' % (pin, pin)) + command
    print('Run ' + str(repetitions) + 'x: ' + command)

    join_times = []
    matches = []
    for _ in range(repetitions):
        # Own process group, so a timeout kills the app too - a survivor would
        # keep its EPC pages and wreck every later point.
        proc = subprocess.Popen(command, cwd='../', shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                start_new_session=True)
        try:
            out, err = proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            print('TIMEOUT after ' + str(timeout_s) + 's - killing process group')
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            proc.communicate()
            append_row(failure_row(cfg, pin, 'timeout'))
            return 'timeout'

        stdout = out.decode('utf-8')
        print(stdout)
        if proc.returncode != 0:
            print('App error (rc=%d):\n%s' % (proc.returncode,
                                              err.decode('utf-8')))
            append_row(failure_row(cfg, pin, 'error'))
            return 'error'

        for line in stdout.splitlines():
            if 'joinTotalTime' in line:
                join_times.append(int(escape_ansi(line.split(": ", 1)[1])))
            elif 'Join matches' in line:
                matches.append(int(escape_ansi(line.split(": ", 1)[1])))

    if not join_times:
        print('No joinTotalTime in output - recording as error')
        append_row(failure_row(cfg, pin, 'error'))
        return 'error'

    join_time = statistics.median(join_times)        # microseconds
    tuples = streamed_tuples(cfg)
    throughput = tuples / join_time                  # M rec/s
    latency = 1 / throughput if throughput > 0 else -1   # us per tuple
    match_count = statistics.median(matches) if matches else -1

    result = ','.join([cfg.algorithm, str(cfg.r_window), str(cfg.r_batch),
                       str(cfg.nthreads), pin, str(tuples), str(join_time),
                       '%.6f' % throughput, '%.4f' % latency,
                       str(int(match_count)), 'ok'])
    print('Join results: ' + result)
    append_row(result)
    return 'ok'


def check_matches(data):
    """A join's answer must not depend on how many threads computed it. Print
    any series where it does - that is a correctness bug, not a slow point."""
    bad = []
    for (alg, window, batch, pin), df in data.groupby(
            ['algorithm', 'window', 'batch', 'pin']):
        counts = df[df['status'] == 'ok']['matches'].unique()
        if len(counts) > 1:
            bad.append((alg, window, batch, pin, sorted(counts)))
    if not bad:
        return
    print('\nWARNING: match count varies with thread count:')
    for alg, window, batch, pin, counts in bad:
        print('  %-12s window=%d batch=%d pin=%s  matches=%s'
              % (alg, window, batch, pin, counts))


def plot(algs, out_file, window, batch, pin='none'):
    """Two panels: absolute throughput, and speedup over the 1-thread point of
    the same series with the ideal line for reference."""
    data = load_results()
    if data is None:
        print('Nothing to plot: %s is missing or empty. Set experiment: 1 in '
              'config.yaml and run the sweep first.' % res_file)
        return
    data = data[(data['algorithm'].isin(algs)) & (data['status'] == 'ok')
                & (data['window'] == window) & (data['batch'] == batch)
                & (data['pin'].astype(str) == str(pin))]
    if not len(data):
        print('Nothing to plot for %s at window=%d batch=%d pin=%s'
              % (algs, window, batch, pin))
        return

    plt.rcParams.update(plt.rcParamsDefault)
    fig, axes = plt.subplots(1, 2, figsize=(7, 3))
    for algorithm in algs:
        df = data[data['algorithm'] == algorithm].sort_values('threads')
        if not len(df):
            continue
        color = color_categorical(algorithm)
        # CSV keeps M rec/s; the paper's plots are in K rec/s.
        axes[0].plot(df['threads'], 1000 * df['throughput'], label=algorithm,
                     marker='o', color=color)
        base = df[df['threads'] == df['threads'].min()]['throughput'].iloc[0]
        axes[1].plot(df['threads'], df['throughput'] / base, label=algorithm,
                     marker='o', color=color)

    threads = sorted(data['threads'].unique())
    axes[1].plot(threads, threads, linestyle='--', color='gray', label='ideal')

    # Mark the NUMA boundary. Only a short tag goes on the line itself (a
    # sentence rotated 90 degrees does not fit in a 3-inch panel); the reason
    # goes in the title, where it can be read horizontally. The line means
    # different things per series, hence the two wordings.
    show_numa = min(threads) < SOCKET_CORES < max(threads)
    if str(pin) == 'none':
        numa_why = ('beyond it threads cross to the 2nd socket, whose EPC is '
                    'remote')
    else:
        numa_why = 'beyond it threads double up on SMT siblings, still local'
    for ax in axes:
        if show_numa:
            ax.axvline(SOCKET_CORES, linestyle=':', color='firebrick',
                       linewidth=1.2, zorder=0)
            # Sits low in the panel: both metrics rise with thread count, so
            # the space under the curves is the empty part of the plot.
            ax.text(SOCKET_CORES * 0.94, 0.04, 'NUMA boundary',
                    transform=ax.get_xaxis_transform(), rotation=90,
                    va='bottom', ha='right', fontsize=7, color='firebrick')
        ax.set_xlabel('threads')
        ax.set_xscale('log', base=2)
        # SOCKET_CORES is close enough to the neighbouring power of two that
        # both tick labels would overlap; the marker line identifies it instead.
        ticks = [t for t in threads if t != SOCKET_CORES]
        ax.set_xticks(ticks)
        ax.set_xticklabels([str(t) for t in ticks])
        ax.set_xticks([], minor=True)
    axes[0].set_ylabel('Throughput [K rec / s]')
    axes[0].set_yscale('log')
    axes[1].set_ylabel('Speedup over 1 thread')
    axes[1].set_yscale('log', base=2)
    axes[1].legend(fontsize='x-small')
    title = 'window %d, batch %d, pin %s' % (window, batch, pin)
    if show_numa:
        title += ('\nNUMA boundary: %d cores per socket - %s'
                  % (SOCKET_CORES, numa_why))
    fig.suptitle(title, fontsize='small')
    fig.tight_layout()
    savefig(out_file)


def plot_speedup(algs, out_file, window, batch, pin='none'):
    """The speedup panel on its own: axes, axis labels and legend, no title.
    This is the figure meant for the paper; plot() above pairs it with absolute
    throughput for eyeballing the two together."""
    data = load_results()
    if data is None:
        print('Nothing to plot: %s is missing or empty.' % res_file)
        return
    data = data[(data['algorithm'].isin(algs)) & (data['status'] == 'ok')
                & (data['window'] == window) & (data['batch'] == batch)
                & (data['pin'].astype(str) == str(pin))]
    if not len(data):
        print('Nothing to plot for %s at window=%d batch=%d pin=%s'
              % (algs, window, batch, pin))
        return

    plt.rcParams.update(plt.rcParamsDefault)
    plt.figure(figsize=(3.3, 2.7))
    ax = plt.gca()
    for algorithm in algs:
        df = data[data['algorithm'] == algorithm].sort_values('threads')
        if not len(df):
            continue
        base = df[df['threads'] == df['threads'].min()]['throughput'].iloc[0]
        ax.plot(df['threads'], df['throughput'] / base, label=algorithm,
                marker='o', color=color_categorical(algorithm))

    threads = sorted(data['threads'].unique())
    ax.plot(threads, threads, linestyle='--', color='gray', label='ideal')

    if min(threads) < SOCKET_CORES < max(threads):
        ax.axvline(SOCKET_CORES, linestyle=':', color='firebrick',
                   linewidth=1.2, zorder=0)
        ax.text(SOCKET_CORES * 0.94, 0.04, 'NUMA boundary',
                transform=ax.get_xaxis_transform(), rotation=90,
                va='bottom', ha='right', fontsize=7, color='firebrick')

    ax.set_xlabel('threads')
    ax.set_ylabel('Speedup over 1 thread')
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)
    ticks = [t for t in threads if t != SOCKET_CORES]
    ax.set_xticks(ticks)
    ax.set_xticklabels([str(t) for t in ticks])
    ax.set_xticks([], minor=True)
    ax.legend(fontsize='x-small')
    savefig(out_file)


def dry_run(algorithms, window, batch, batches, threads, pin, timeout_s,
            repetitions):
    """Print the plan with a runtime estimate. Per-batch cost is what it is;
    the estimate below extrapolates it from measured points at window 2^22 and
    assumes n*log2(n) scaling in the merge, so treat it as an order of
    magnitude, not a promise."""
    already = done_configs()
    # Measured single-thread seconds per batch at window 2^22, batch 4096.
    per_batch_at_4m = {'FK-MERG-L4': 2.33, 'FK-MERG-L3': 2.33,
                       'FK-SORT-L4': 5.6, 'FK-SORT-L3': 5.6}
    ref = 2 ** 22

    def est_seconds(algorithm, nthreads):
        base = per_batch_at_4m.get(algorithm)
        if base is None:
            return None
        # merge work ~ n log n, so scale per-batch cost by that ratio
        scale = ((window * (window.bit_length())) /
                 (ref * (ref.bit_length())))
        # observed speedup saturates around 6x, so cap it
        speedup = min(nthreads, 6.0) ** 0.92
        return (base * scale * batches / speedup) + ENCLAVE_INIT_S

    print('Planned: %d algorithms x %d thread counts = %d runs, %dx each'
          % (len(algorithms), len(threads),
             len(algorithms) * len(threads), repetitions))
    print('window %d, batch %d, %d batches (%d tuples streamed per run), '
          'pin=%s, timeout %ds'
          % (window, batch, batches,
             2 * batches * batch, pin, timeout_s))
    print()
    head = '%-12s %8s %10s %8s' % ('algorithm', 'threads', 'est. time', 'status')
    print(head)
    print('-' * len(head))
    total = 0.0
    for algorithm in algorithms:
        for nthreads in threads:
            secs = est_seconds(algorithm, nthreads)
            if (algorithm, window, batch, nthreads, pin) in already:
                status = 'done'
            else:
                status = 'ok'
                total += (secs or 0) * repetitions
            print('%-12s %8d %10s %8s'
                  % (algorithm, nthreads,
                     ('%.0fs' % secs) if secs else 'n/a', status))
    print()
    print('Estimated total for runs marked ok: %.1f min' % (total / 60))
    print('(includes ~%ds of enclave init per run)' % ENCLAVE_INIT_S)


# Enclave creation alone costs this much per ./app invocation on the SGX box,
# independent of the join - it dominates the plan for cheap points.
ENCLAVE_INIT_S = 20


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='print the plan with runtime estimates, run nothing')
    parser.add_argument('--algorithms', nargs='+', default=ALGORITHMS)
    parser.add_argument('--threads', nargs='+', type=int, default=THREADS)
    parser.add_argument('--window', type=int, default=WINDOW)
    parser.add_argument('--batch', type=int, default=BATCH)
    parser.add_argument('--batches', type=int, default=BATCHES,
                        help='batches streamed past the window fill (default: %d)'
                             % BATCHES)
    parser.add_argument('--pin-node', default='none',
                        help='NUMA node to confine CPUs and memory to, or '
                             '"none" (default) to leave it to the scheduler')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT_S,
                        help='per-run timeout in seconds (default: 1h)')
    args = parser.parse_args()

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    ensure_header()

    if args.dry_run:
        dry_run(args.algorithms, args.window, args.batch, args.batches,
                args.threads, args.pin_node, args.timeout,
                config['repetitions'])
        sys.exit(0)

    if config['experiment']:
        if config['compile']:
            compile_app()

        already = done_configs()
        for algorithm in args.algorithms:
            for nthreads in sorted(args.threads):
                key = (algorithm, args.window, args.batch, nthreads,
                       args.pin_node)
                if key in already:
                    print('Skip (already done): %s %d threads'
                          % (algorithm, nthreads))
                    continue
                cfg = build_config(algorithm, args.window, args.batch,
                                   args.batches, nthreads)
                status = join(cfg, args.pin_node, config['repetitions'],
                              args.timeout)
                if status == 'timeout':
                    # More threads should only ever be faster; a timeout at 1
                    # thread says the config is too big, so stop this series.
                    if nthreads == min(args.threads):
                        print('Abandoning %s - even 1 thread timed out'
                              % algorithm)
                        break

    if config['plot']:
        data = load_results()
        if data is not None:
            check_matches(data)
        suffix = '' if args.pin_node == 'none' else '-node' + str(args.pin_node)
        plot(['FK-MERG-L3', 'FK-MERG-L4', 'FK-SORT-L3', 'FK-SORT-L4'],
             'results/thread-scalability%s.png' % suffix, args.window,
             args.batch, args.pin_node)
        plot(['FK-MERG-L4', 'FK-SORT-L4'],
             'results/thread-scalability-l4%s.png' % suffix, args.window,
             args.batch, args.pin_node)
        plot_speedup(['FK-MERG-L3', 'FK-MERG-L4', 'FK-SORT-L3', 'FK-SORT-L4'],
                     'results/thread-scalability-speedup%s.png' % suffix,
                     args.window, args.batch, args.pin_node)
