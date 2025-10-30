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
    result = cfg.algorithm + ',' + str(cfg.nthreads) + ',' + str(cfg.r_size+cfg.s_size) + ',' + str(cfg.r_window) + ',' + str(joinTime) + ',' + str(thr)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(algs, out_file):
    data = pd.read_csv(res_file)
    plt.figure(figsize=(3, 3))
    data = data[data['algorithm'].isin(algs)]

    windows = data['window'].unique()

    for i in range(len(windows)):
        # plt.subplot(1, len(windows), i+1)
        local_data = data[data['window'] == windows[i]]
        for algorithm in algs:
            df = local_data[(local_data['algorithm'] == algorithm)]
            threads = df['threads']
            # throughputs = (cfg.r_size+cfg.s_size)/df['time']
            base = df['throughput'][0]
            throughputs = df['throughput']/base
            plt.plot(threads, throughputs, label=algorithm, marker='o',
                     color=color_categorical(algorithm))
        plt.legend(fontsize='x-small')
        # plt.title('Window: ' + str(windows[i]))
        plt.xlabel('threads')
        # plt.yscale('log')
        # plt.ylim(bottom=25, top= 32)
        # plt.xlim(right=16)
        # if i == 0:
        plt.ylabel('Scalability')
    savefig(out_file)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algorithms = ['FK-EPHI-L2']
    cfg = Config(name='synth-1',
                    algorithm = None,
                    r_rate=1000,
                    s_rate=1000,
                    r_size=5000000,
                    s_size=5000000,
                    r_window=4194304,
                    s_window=4194304,
                    r_batch=4096,
                    s_batch=4096,
                    skew=0,
                    fk_join=True)

    threads = ([2**n for n in range(7)])

    if config['experiment']:
        remove_file(res_file)
        init_file(res_file, "algorithm,threads,totalInputTuples,window,time,throughput\n")
        if config['compile']:
            compile_app()
        for algorithm in algorithms:
            cfg.algorithm = algorithm
            cfg.no_sgx = True if algorithm == 'SHJ' or algorithm=='SHJ-L0' else False
            for num_threads in threads:
                cfg.nthreads = num_threads
                join(cfg, config['repetitions'])

    if config['plot']:
        plot(['FK-MERG-L3'], 'results/l3-scalability.png')
        plot(['FK-MERG-L4'], 'results/l4-scalability.png')
