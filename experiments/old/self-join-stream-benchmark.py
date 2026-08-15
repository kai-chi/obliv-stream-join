#!/usr/bin/python3
import os
import statistics
import subprocess

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

import commons

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'
img_file = 'results/' + filename + '.png'


def join(exp_config: commons.Config, repetitions: int):
    print('Run self-join stream ' + algorithm + ' with size = ' + str(size) + ' and batch = ' + str(batch) + ' '
          + str(repetitions) + ' times')
    joinTimes = []
    throughputs = []

    for i in range(repetitions):
        print('Command: ' + exp_config.command())
        try:
            stdout = subprocess.check_output(exp_config.command(), cwd='../', shell=True, stderr=subprocess.DEVNULL) \
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            print("App error:\n", e.output)
            print(e.stdout)
            exit()
        print(stdout)
        for line in stdout.splitlines():
            if 'joinTotalTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                joinTimes.append(time)
            elif 'joinThroughput' in line:
                throughput = float(commons.escape_ansi(line.split(": ", 1)[1]))
                throughputs.append(throughput)

    joinTime = statistics.median(joinTimes)
    thr = statistics.median(throughputs)
    result = algorithm + ',' + str(size) + ',' + str(batch) + ',' + str(joinTime) + ',' + str(thr)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(cfg: commons.Config):
    data = pd.read_csv(res_file)
    # data = data[data['algorithm'].str.contains('J2')]
    plt.figure(figsize=(5, 3))

    for algorithm in data['algorithm'].unique():
        df = data[data['algorithm'] == algorithm]
        sizes = df['sizeRelation']
        throughputs = 2*df['sizeRelation']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o')

    plt.title('Primary-key self-join stream comparison\n with batch=' + str(cfg.batch) +
              ', windows size=' + str(cfg.r_window))
    plt.xlabel('Input table size |R|')
    plt.ylabel('Throughput [M rec / s]')
    plt.xscale('log')
    plt.yscale('log')
    plt.legend(fontsize='x-small', ncol=2)
    # commons.draw_horizontal_lines(plt, [3600*12])
    # plt.text(1000, 3600*13,'12h')
    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # algorithms = ['J0', 'J2', 'J3', 'J4', 'KRASStream', 'SHJ_Graphos']
    algorithms = ['SHJ', 'J0', 'J3', 'J4', 'J2']
    sizes = 512 * (2 ** np.arange(12))
    batch = 100
    cfg = commons.Config(batch=batch, r_window=0, s_window=0)

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "algorithm,sizeRelation,batchSize,time,throughput\n")
            commons.compile_app()
        for algorithm in algorithms:
            for size in sizes:
                exp_config = commons.Config(algorithm=algorithm, r_size=size, s_size=size,
                                            r_window=size, s_window=size, r_rate=1000,
                                            s_rate=1000, fk_join=True, self_join=True,
                                            batch=batch)
                join(exp_config, config['repetitions'])

    if config['plot']:
        plot(cfg)
