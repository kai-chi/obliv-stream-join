#!/usr/bin/python3
import os
import statistics
import subprocess

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from commons import *

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'
img_file = 'results/' + filename + '.png'


def join(cfg: Config, repetitions):
    print('Run join stream ' + str(repetitions) + ' times with ' + str(cfg))
    joinTimes = []
    throughputs = []

    for i in range(repetitions):
        print('Command: ' + cfg.command())

        # stdout = ''
        try:
            stdout = subprocess.check_output(cfg.command(), cwd='../', shell=True, stderr=subprocess.DEVNULL) \
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            print("App error:\n", e.output)
            print(e.stdout)
            exit()
        print(stdout)
        for line in stdout.splitlines():
            if 'joinTotalTime' in line:
                time = int(escape_ansi(line.split(": ", 1)[1]))
                joinTimes.append(time)
            elif 'joinThroughput' in line:
                throughput = float(escape_ansi(line.split(": ", 1)[1]))
                throughputs.append(throughput)

    joinTime = statistics.median(joinTimes)
    thr = statistics.median(throughputs)
    result = (str(cfg.algorithm) + ',' + str(cfg.dataset) + ',' + str(cfg.r_window) + ',' + str(cfg.r_batch) + ','
              + str(cfg.r_size+cfg.s_size) + "," + str(joinTime) + ',' + str(thr))
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(algs, baseline, out_file):
    plt.rcParams.update(plt.rcParamsDefault)

    shj_l2_throughput = 29.55e-6 # M rec/s
    shj_l3_throughput = 29.55e-6 # M rec/s
    shj_l4_throughput = 29.55e-6 # M rec/s
    nlj_throughput = 4.02431 # K rec/s

    data = pd.read_csv(res_file)
    plt.figure(figsize=(3, 3))

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['num-tuples'] / d['time']
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    if baseline == 'SHJ-L2':
        draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
        # plt.title('L2: micro-batch size')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)
        draw_horizontal_line(plt, nlj_throughput, color=color_categorical(baseline))
        plt.text(100, nlj_throughput*1.2, 'NLJ-L4')
        # plt.title('L3: micro-batch size')
    plt.xlabel('Batch size [rec]')
    plt.ylabel('Throughput [K rec / s]')
    plt.xscale('log')
    plt.yscale('log')
    plt.legend(fontsize='small')

    # plt.ylim(top=1, bottom=0.001)

    # draw_horizontal_lines(plt, [3600*12])
    # plt.text(1000, 3600*13,'12h')

    savefig(out_file)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # (algorithm name, FK-join)
    algorithms = ['FK-MERG-L4', 'FK-MERG-L3', 'FK-EPHI-L2',
                  'FK-SORT-L4', 'FK-SORT-L3',]
    algorithms = ['SHJ-L3', 'SHJ-L4']

    # datasets = ['synth-1', 'synth-2', 'tpch-1']
    datasets = ['synth-1']
    batches = evenly_log_distributed_points(100, 50000, 10)


    if config['experiment']:
        remove_file(res_file)
        init_file(res_file, "algorithm,dataset,window,batch,num-tuples,time,throughput\n")
        if config['compile']:
            compile_app()
        for algorithm in algorithms:
            for dataset in datasets:
                for batch in batches:
                    cfg = Config(algorithm=algorithm, dataset=dataset, r_window=65536, s_window=65536,
                                 r_batch=batch, s_batch=batch, fk_join=True)
                    if algorithm == 'NFK-JOIN-L3' or algorithm == 'NFK-JOIN-L2':
                        cfg.fk_join = False
                    cfg.r_size = 200000
                    cfg.s_size = 200000
                    join(cfg, config['repetitions'])

    if config['plot']:
        # plot(['FK-EPHI-L2'], 'SHJ-L2', 'results/l2-batch-size.png')
        # plot(['FK-MERG-L3'], 'SHJ-L3', 'results/l3-batch-size.png')
        # plot(['FK-MERG-L4'], 'SHJ-L4', 'results/l4-batch-size.png')
        plot(['NFK-JOIN-L3'], 'SHJ-L4', 'results/nfk-batch-size.png')

