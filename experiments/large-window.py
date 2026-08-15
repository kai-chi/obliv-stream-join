#!/usr/bin/python3
"""
Large-window scalability sweep.

Answers the reviewer question "how do the OCA joins behave at realistic window
sizes?" - the paper's default window is 2^16 because that is where the ORAM
baselines (SHJ-L2/L3/L4, FK-MERG-L2, FK-SORT-L2, NFK-JOIN-L2) still finish
inside the 6h budget. The non-ORAM algorithms are not bound by that, so this
script drops the ORAM family entirely and sweeps the survivors from 2^20 up to
2^25 tuples per window.

Setup, fixed across the sweep:

    window        2^20 .. 2^25 tuples, per stream
    batch         2^16 tuples, per stream
    input         4 * window tuples per stream (so every point retires the
                  window ~3 times over and we measure steady state, not the
                  startup fill)
    dataset       synth-1 (uniform FK) by default; synth-2 (skewed) also works.
                  tpch-1 does NOT - it reads customer/orders at SF 0.1, i.e.
                  1.5M order tuples, which caps the window at ~2^20.

Runtime is the thing to watch here, not correctness. At 2^25 a single point
feeds 2.7e8 tuples through the join; at FK-MERG's measured ~10^5 tuples/s that
is hours. Run `./large-window.py --dry-run` first - it prints the planned
configs with a runtime and enclave-memory estimate extrapolated from
results/window-size.csv, so you can decide what to cut before committing a
machine for two days.

The sweep is restartable: results are appended, and a config that already has a
row in the CSV is skipped. Kill it and rerun and it picks up where it stopped.

Usage:

    ./large-window.py --dry-run        # plan only, no app runs
    ./large-window.py                  # honours config.yaml (experiment/plot)
    ./large-window.py --timeout 3600   # per-run budget in seconds
"""
import argparse
import math
import os
import re
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
img_file = 'results/' + filename + '.png'

# time is joinTotalTime in microseconds and throughput is joinThroughput in
# M rec/s, both scraped from the app's own log lines (as window-size.py does).
# latency is 1/throughput, i.e. microseconds per tuple - the Table II metric.
HEADER = ('algorithm,dataset,window,batch,input-tuples,time,throughput,'
          'latency,peak-rss-kb,status\n')

# Sweep parameters. Windows are per stream, and so is the batch.
WINDOWS = [2 ** n for n in range(20, 26)]
BATCH = 2 ** 16
INPUT_FACTOR = 4  # tuples generated per stream = INPUT_FACTOR * window
DATASETS = ['synth-1']
ALGORITHMS = ['FK-MERG-L3', 'FK-MERG-L4', 'FK-SORT-L3', 'FK-SORT-L4',
              'NFK-JOIN-L3']

# Paper's abort rule: anything past 6h is reported as infeasible.
DEFAULT_TIMEOUT_S = 6 * 60 * 60

# Enclave heap ceiling, read from Enclave.config.xml so the estimate below
# tracks whatever the enclave was actually built with.
ENCLAVE_CONFIG = '../Enclave/Enclave.config.xml'

# sizeof() of the two element types the joins keep windows in, including
# padding (see Include/data-types.h). row_t is what a window holds; the SORT
# family additionally materializes the R+S window as tagged row_table_t before
# sorting it.
SIZEOF_ROW_T = 24        # timespec_t(16) + key(4) + payload(4)
SIZEOF_ROW_TABLE_T = 32  # + table_id(1), padded to 8

# Very rough per-family multiplier on top of the two resident windows,
# covering the temporaries each algorithm allocates per batch (OAppend's
# padded batch and merge buffer, the sort scratch array, the marked/prefix-sum
# arrays in OCA.cpp). Only used by --dry-run to flag configs that will not fit
# in the enclave heap - it is a planning aid, not a measurement.
MEM_FACTOR = {
    'FK-MERG-L3': 3.0,
    'FK-MERG-L4': 3.0,
    'FK-SORT-L3': 5.0,
    'FK-SORT-L4': 5.0,
    'NFK-JOIN-L3': 6.0,
}


def is_non_fk(algorithm):
    return algorithm.startswith('NFK-') or algorithm == 'NLJ-L4'


def build_config(algorithm, dataset, window):
    """One sweep point. Window and batch are set explicitly, so the --dataset
    flag only carries the distribution (uniform vs skewed FK)."""
    cfg = Config(algorithm=algorithm)
    cfg.dataset = dataset
    cfg.fk_join = not is_non_fk(algorithm)
    cfg.r_window = window
    cfg.s_window = window
    cfg.r_batch = BATCH
    cfg.s_batch = BATCH
    cfg.r_size = INPUT_FACTOR * window
    cfg.s_size = INPUT_FACTOR * window
    return cfg


def append_row(row):
    with open(res_file, 'a') as f:
        f.write(row + '\n')


def ensure_header():
    """Make sure the results CSV starts with HEADER.

    Existence is not enough to go on: an empty file left by `touch`, or one
    whose header got lost, makes pandas read the first data row as the column
    names. Create the file if it is missing, and prepend the header if the
    first line is data.
    """
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
    """The results CSV, or None if it is missing or has no rows yet. The sweep
    is long enough that half-finished and not-yet-started states are normal, so
    every reader goes through here instead of calling read_csv directly."""
    if not os.path.exists(res_file):
        return None
    try:
        data = pd.read_csv(res_file)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None
    return data if len(data) else None


def done_configs():
    """(algorithm, dataset, window) triples already in the CSV, so a restarted
    sweep does not redo them. Timed-out and failed rows count as done."""
    data = load_results()
    if data is None:
        return set()
    return {(r['algorithm'], r['dataset'], int(r['window']))
            for _, r in data.iterrows()}


def parse_peak_rss(stderr_text):
    """Peak RSS of the app process, in kB, from `/usr/bin/time -v`. This is the
    untrusted-side footprint; it includes the EPC pages backing the enclave but
    is not the same number as the enclave-internal heap high-water mark that
    Table II reports via sgx-gdb. Recorded as a cheap, scriptable proxy."""
    match = re.search(r'Maximum resident set size \(kbytes\): (\d+)',
                      stderr_text)
    return int(match.group(1)) if match else -1


def join(cfg, repetitions, timeout_s, measure_memory=True):
    """Run one config `repetitions` times, append the median to the CSV.

    Returns 'ok', 'timeout' or 'error'. Unlike the other sweep scripts this
    does not exit() on failure - a sweep this long should record the failure
    and keep going.
    """
    print('Run join stream ' + str(repetitions) + ' times with ' + str(cfg))
    join_times = []
    throughputs = []
    peak_rss = []

    command = cfg.command()
    if measure_memory:
        command = '/usr/bin/time -v ' + command

    for _ in range(repetitions):
        print('Command: ' + command)
        # start_new_session puts the shell and the app in their own process
        # group, so a timeout can kill the whole group. Killing just the shell
        # would leave the app running and holding its EPC pages, which would
        # then wreck every later point in the sweep.
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
            append_row(','.join([cfg.algorithm, str(cfg.dataset),
                                 str(cfg.r_window), str(cfg.r_batch),
                                 str(cfg.r_size + cfg.s_size),
                                 '-1', '-1', '-1', '-1', 'timeout']))
            return 'timeout'

        stdout = out.decode('utf-8')
        stderr = err.decode('utf-8')
        print(stdout)
        if proc.returncode != 0:
            print('App error (rc=' + str(proc.returncode) + '):\n' + stderr)
            append_row(','.join([cfg.algorithm, str(cfg.dataset),
                                 str(cfg.r_window), str(cfg.r_batch),
                                 str(cfg.r_size + cfg.s_size),
                                 '-1', '-1', '-1', '-1', 'error']))
            return 'error'

        # Same two lines window-size.py scrapes: joinTotalTime in microseconds
        # and joinThroughput in M rec/s, both as the app computed them.
        for line in stdout.splitlines():
            if 'joinTotalTime' in line:
                join_times.append(int(escape_ansi(line.split(": ", 1)[1])))
            elif 'joinThroughput' in line:
                throughputs.append(float(escape_ansi(line.split(": ", 1)[1])))
        if measure_memory:
            peak_rss.append(parse_peak_rss(stderr))

    if not join_times or not throughputs:
        print('No joinTotalTime/joinThroughput in output - recording as error')
        append_row(','.join([cfg.algorithm, str(cfg.dataset),
                             str(cfg.r_window), str(cfg.r_batch),
                             str(cfg.r_size + cfg.s_size),
                             '-1', '-1', '-1', '-1', 'error']))
        return 'error'

    join_time = statistics.median(join_times)          # microseconds
    throughput = statistics.median(throughputs)        # M rec/s, from the app
    # M rec/s is tuples per microsecond, so its reciprocal is the per-tuple
    # latency in microseconds - the Table II metric.
    latency = 1 / throughput if throughput > 0 else -1
    # Tuples that actually stream through the join, i.e. everything past the
    # initial window fill - same convention as window-size.py.
    input_tuples = (cfg.r_size + cfg.s_size - cfg.r_window - cfg.s_window)
    rss = statistics.median(peak_rss) if peak_rss else -1

    result = ','.join([cfg.algorithm, str(cfg.dataset), str(cfg.r_window),
                       str(cfg.r_batch), str(input_tuples), str(join_time),
                       '%.6f' % throughput, '%.4f' % latency, str(rss), 'ok'])
    print('Join results: ' + result)
    append_row(result)
    return 'ok'


def enclave_heap_bytes():
    """HeapMaxSize from the enclave config, or None if it cannot be read."""
    try:
        with open(ENCLAVE_CONFIG) as f:
            text = f.read()
    except OSError:
        return None
    # Skip commented-out entries - the config keeps an old value in a comment.
    text = re.sub(r'<!--.*?-->', '', text, flags=re.S)
    match = re.search(r'<HeapMaxSize>\s*(0x[0-9a-fA-F]+|\d+)\s*</HeapMaxSize>',
                      text)
    return int(match.group(1), 0) if match else None


def estimate_memory_bytes(algorithm, window):
    """Coarse enclave-heap estimate for one config. See MEM_FACTOR."""
    element = (SIZEOF_ROW_TABLE_T if 'SORT' in algorithm or 'NFK' in algorithm
               else SIZEOF_ROW_T)
    resident = 2 * (window + BATCH) * element
    return int(resident * MEM_FACTOR.get(algorithm, 3.0))


def estimate_seconds(algorithm, dataset, window):
    """Extrapolate runtime from results/window-size.csv.

    Throughput of these algorithms falls roughly log-linearly in the window
    size, so fit a line through log10(throughput) vs log10(window) on the two
    largest measured points and extend it. Rough by construction - it is there
    to tell 20 minutes from 20 hours, nothing finer.
    """
    src = 'results/window-size.csv'
    if not os.path.exists(src):
        return None
    try:
        data = pd.read_csv(src)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None
    df = data[(data['algorithm'] == algorithm) & (data['dataset'] == dataset)]
    if len(df) < 2:
        return None
    df = df.sort_values('window').tail(2)
    windows = df['window'].tolist()
    thr = [1000 * t / ti for t, ti in zip(df['input-tuples'], df['time'])]
    if windows[0] == windows[1] or thr[0] <= 0 or thr[1] <= 0:
        return None
    slope = ((math.log10(thr[1]) - math.log10(thr[0])) /
             (math.log10(windows[1]) - math.log10(windows[0])))
    log_thr = math.log10(thr[1]) + slope * (math.log10(window) -
                                            math.log10(windows[1]))
    predicted = 10 ** log_thr  # K tuples/s
    input_tuples = 2 * INPUT_FACTOR * window - 2 * window
    return input_tuples / (predicted * 1000)


def human_time(seconds):
    if seconds is None:
        return 'n/a'
    if seconds < 60:
        return '%.0fs' % seconds
    if seconds < 3600:
        return '%.1fm' % (seconds / 60)
    return '%.1fh' % (seconds / 3600)


def dry_run(algorithms, datasets, windows, timeout_s):
    heap = enclave_heap_bytes()
    already = done_configs()
    print('Planned sweep: %d algorithms x %d datasets x %d windows = %d runs'
          % (len(algorithms), len(datasets), len(windows),
             len(algorithms) * len(datasets) * len(windows)))
    print('Batch %d tuples, input %dx window per stream, timeout %s per run'
          % (BATCH, INPUT_FACTOR, human_time(timeout_s)))
    if heap:
        print('Enclave HeapMaxSize: %.1f GiB' % (heap / 2 ** 30))
    print()
    header = ('%-14s %-9s %10s %14s %12s %10s %8s' %
              ('algorithm', 'dataset', 'window', 'input tuples', 'est. mem',
               'est. time', 'status'))
    print(header)
    print('-' * len(header))
    total = 0.0
    for algorithm in algorithms:
        for dataset in datasets:
            for window in windows:
                mem = estimate_memory_bytes(algorithm, window)
                secs = estimate_seconds(algorithm, dataset, window)
                if (algorithm, dataset, window) in already:
                    status = 'done'
                elif heap and mem > heap:
                    status = 'OOM?'
                elif secs and secs > timeout_s:
                    status = 'slow'
                else:
                    status = 'ok'
                    total += secs or 0
                print('%-14s %-9s %10d %14d %11.1fG %10s %8s'
                      % (algorithm, dataset, window,
                         2 * INPUT_FACTOR * window - 2 * window,
                         mem / 2 ** 30, human_time(secs), status))
    print()
    print('Estimated total for runs marked ok: ' + human_time(total))
    print('Rows marked OOM? need a bigger <HeapMaxSize> in '
          + ENCLAVE_CONFIG + '; rows marked slow exceed the per-run timeout.')


def plot(algs, out_file, metric='throughput'):
    data = load_results()
    if data is None:
        print('Nothing to plot: ' + res_file + ' is missing or empty. Set '
              'experiment: 1 in config.yaml and run the sweep first.')
        return
    data = data[(data['algorithm'].isin(algs)) & (data['status'] == 'ok')]
    if not len(data):
        print('Nothing to plot for ' + str(algs) + ' - no successful runs in '
              + res_file)
        return
    plt.rcParams.update(plt.rcParamsDefault)
    plt.figure(figsize=(3.5, 3))
    for algorithm in data['algorithm'].unique():
        df = data[data['algorithm'] == algorithm].sort_values('window')
        # The CSV keeps the app's M rec/s; the paper's plots are in K rec/s.
        values = 1000 * df[metric] if metric == 'throughput' else df[metric]
        plt.plot(df['window'], values, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    plt.legend(fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.xscale('log', base=2)
    plt.yscale('log')
    plt.ylabel('Throughput [K rec / s]' if metric == 'throughput'
               else 'Latency [$\\mu$s / tuple]')
    savefig(out_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='print the plan with runtime/memory estimates, '
                             'run nothing')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT_S,
                        help='per-run timeout in seconds (default: 6h)')
    parser.add_argument('--no-memory', action='store_true',
                        help='skip the /usr/bin/time -v wrapper')
    parser.add_argument('--algorithms', nargs='+', default=ALGORITHMS)
    parser.add_argument('--datasets', nargs='+', default=DATASETS)
    parser.add_argument('--max-exp', type=int, default=25,
                        help='largest window exponent to run (default: 25)')
    args = parser.parse_args()

    windows = [w for w in WINDOWS if w <= 2 ** args.max_exp]

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Do this before anything reads the CSV - dry-run, the resume check and
    # the plots all parse it.
    ensure_header()

    if args.dry_run:
        dry_run(args.algorithms, args.datasets, windows, args.timeout)
        sys.exit(0)

    if config['experiment']:
        # Deliberately not remove_file() - this sweep is long enough that
        # resuming matters more than a clean slate. Delete the CSV by hand for
        # a fresh run.
        if config['compile']:
            compile_app()

        already = done_configs()
        for algorithm in args.algorithms:
            for dataset in args.datasets:
                # Windows ascending: once one times out or OOMs, every larger
                # window for the same algorithm will too, so stop that series.
                for window in sorted(windows):
                    if (algorithm, dataset, window) in already:
                        print('Skip (already done): %s %s %d'
                              % (algorithm, dataset, window))
                        continue
                    cfg = build_config(algorithm, dataset, window)
                    status = join(cfg, config['repetitions'], args.timeout,
                                  measure_memory=not args.no_memory)
                    if status != 'ok':
                        print('Abandoning %s on %s at window %d (%s) - larger '
                              'windows will not do better'
                              % (algorithm, dataset, window, status))
                        break

    if config['plot']:
        plot(['FK-MERG-L3', 'FK-MERG-L4', 'FK-SORT-L3', 'FK-SORT-L4'],
             'results/large-window-fk.png')
        plot(['FK-MERG-L3', 'FK-MERG-L4', 'FK-SORT-L3', 'FK-SORT-L4'],
             'results/large-window-fk-latency.png', metric='latency')
        plot(['NFK-JOIN-L3'], 'results/large-window-nfk.png')
