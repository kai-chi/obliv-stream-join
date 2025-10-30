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
    result = (algorithm + ',' + str(cfg.dataset) + ',' + str(cfg.r_window) + ',' + str(cfg.r_batch) + ',' + str(joinTime) +
              ',' + str(cfg.r_size+cfg.s_size-cfg.r_window-cfg.s_window))
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(algs, out_file):
    plt.rcParams.update(plt.rcParamsDefault)
    data = pd.read_csv(res_file)
    plt.figure(figsize=(4,4))
    ax = plt.gca()
    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()
    datasets = data['dataset'].unique()

    for i in range(len(datasets)):
        # plt.subplot(1, len(datasets), i+1)
        for algorithm in algorithms:
            df = data[(data['algorithm'] == algorithm) & (data['dataset'] == datasets[i])]
            sizes = df['window']
            throughputs = 1000*df['input-tuples']/df['time']
            plt.plot(sizes, throughputs, label=algorithm, marker='o',
                     color=color_categorical(algorithm))

        plt.xlabel('Window size [tuples]')
        plt.yscale('log')
        plt.xscale('log', base=2)
        # if i == 0:
        plt.ylabel('Throughput [K rec / s]')

        x = np.linspace(2048, 33554432, 100)
        plt.plot(x, (1/(x * np.log(x)))*1500000, label='n log n',color='pink')  # Add n log n curve

        sort_l3 = data[data['algorithm'] == 'FK-SORT-L3']
        merg_l3 = data[data['algorithm'] == 'FK-MERG-L3']
        impr = sort_l3['time']/merg_l3['time'].values
        print(impr)
        ax2 = ax.twinx()
        line2 = ax2.plot(sizes, impr, label='improvement', linestyle='--')
        ax2.set_ylabel('Speedup factor')
        lines = ax.legend().get_lines() + line2
        labels = [l.get_label() for l in lines]
        ax.legend(lines, labels,fontsize='x-small', loc='upper center')
        ax2.set_ylim(bottom=1, top=5)
        plt.title('MERG vs. SORT for m=logN')

    savefig(out_file)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algorithms = ['FK-MERG-L3', 'FK-SORT-L3']
    datasets = ['synth-1']

    windows = evenly_log_distributed_points(2048, 33554432, 15)
    print(windows)

    if config['experiment']:
        remove_file(res_file)
        init_file(res_file, "algorithm,dataset,window,batch,time,input-tuples\n")
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
                    cfg.r_batch = next_power_of_two(int(np.log2(window)))
                    cfg.s_batch = next_power_of_two(int(np.log2(window)))
                    cfg.r_size = int(window) + 10 * int(cfg.r_batch)
                    cfg.s_size = int(window) + 10 * int(cfg.s_batch)

                    ##
                    join(cfg, config['repetitions'])

    if config['plot']:
        plot(['FK-MERG-L3', 'FK-SORT-L3'], 'results/window-batch-size.png')
