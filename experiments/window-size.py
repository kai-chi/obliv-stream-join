#!/usr/bin/python3
import os
import statistics
import subprocess

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
    result = (algorithm + ',' + str(cfg.dataset) + ',' + str(cfg.r_window) + ',' + str(joinTime) +
              ',' + str(cfg.r_size+cfg.s_size-cfg.r_window-cfg.s_window))
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(algs, out_file):
    plt.rcParams.update(plt.rcParamsDefault)
    data = pd.read_csv(res_file)
    plt.figure(figsize=(3,3))
    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()
    datasets = data['dataset'].unique()

    for i in range(1):
        # plt.subplot(1, len(datasets), i+1)
        for algorithm in algorithms:
            df = data[(data['algorithm'] == algorithm) & (data['dataset'] == datasets[i])]
            sizes = df['window']
            # throughputs = (cfg.r_size+cfg.s_size)/df['time']
            throughputs = 1000*df['input-tuples']/df['time']
            plt.plot(sizes, throughputs, label=algorithm, marker='o',
                     color=color_categorical(algorithm))
        plt.legend(fontsize='x-small')
        plt.xlabel('Window size [tuples]')
        plt.yscale('log')
        plt.xscale('log')
        # if i == 0:
        plt.ylabel('Throughput [K rec / s]')
    savefig(out_file)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    #algorithms = ['FK-EPHI-L2', 'FK-MERG-L3', 'SHJ-L2', 'SHJ-L3']
    algorithms = ['SHJ', 'FK-MERG-L4', 'FK-SORT-L4', 'NLJ-L4']
    algorithms = ['SHJ-L1']
    datasets = ['synth-1']

    windows = evenly_log_distributed_points(128, 1048576, 14)
    print(windows)

    if config['experiment']:
        remove_file(res_file)
        init_file(res_file, "algorithm,dataset,window,time,input-tuples\n")
        if config['compile']:
            compile_app()
        for algorithm in algorithms:
            for dataset in datasets:
                for window in windows:
                    cfg = Config(algorithm=algorithm)
                    if algorithm == 'NFK-JOIN-L3' or algorithm == 'NFK-JOIN-L2':
                        cfg.fk_join = False
                    cfg.dataset = dataset
                    cfg.r_window = window
                    cfg.s_window = window
                    if int(cfg.r_window) <= 1024:
                        cfg.r_batch = cfg.r_window / 2
                    else:
                        cfg.r_batch = 1024
                    if int(cfg.s_window) <= 1024:
                        cfg.s_batch = cfg.s_window / 2
                    else:
                        cfg.s_batch = 1024
                    cfg.r_size = int(window) + 4 * int(cfg.r_batch)
                    cfg.s_size = int(window) + 4 * int(cfg.s_batch)

                    ##
                    join(cfg, config['repetitions'])

    if config['plot']:
        # plot(['FK-EPHI-L2', 'SHJ-L2'], 'results/l2-window-size.png')
        # plot(['FK-MERG-L3', 'SHJ-L3'], 'results/l3-window-size.png')
        # plot(['FK-MERG-L4', 'NLJ-L4'], 'results/l4-window-size.png')
        plot(['SHJ', 'SHJ-L0','SHJ-L1','SHJ-L2','SHJ-L3','SHJ-L4'], 'results/shj-window-size.png')
        plot(['SHJ', 'NLJ-L4', 'NFK-JOIN-L2','NFK-JOIN-L3'], 'results/nfk-window-size.png')
