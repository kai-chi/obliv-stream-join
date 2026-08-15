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
img_file = 'results/'  + filename + '.png'


def join(expConfig, repetitions):
    print('Run join stream ' + str(repetitions) + ' times with ' + str(expConfig))
    joinTimes = []
    throughputs = []

    for i in range(repetitions):
        c = commons.Command(expConfig)
        print('Command: ' + c.command())

        # stdout = ''
        try:
            stdout = subprocess.check_output(c.command(), cwd='../', shell=True, stderr=subprocess.DEVNULL) \
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            print ("App error:\n", e.output)
            print (e.stdout)
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
    result = algorithm + ',' + str(expConfig.r_size+expConfig.s_size) + ',' + str(joinTime) + ',' + str(thr)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot():
    data = pd.read_csv(res_file)
    fig = plt.figure(figsize=(5,5))
    algorithms = data['algorithm'].unique()

    for alg in algorithms:
        df = data[data['algorithm'] == alg]
        sizes = df['inputTuples']
        times = df['time']
        plt.plot(sizes, times, label=alg, marker='o')

    plt.title('Stream join throughput')
    plt.xlabel('Total input tuples')
    plt.ylabel('Join Time [micro s]')
    plt.xscale('log')
    plt.yscale('log')
    plt.legend()
    # commons.draw_horizontal_lines(plt, [3600*12])
    # plt.text(1000, 3600*13,'12h')

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    mag = [10**n for n in range(2,5)]
    sizes = commons.flatten([[m, 2.5*m, 5*m, 7.5*m] for m in mag])

    algorithms = ['J0', 'J2', 'J4']
    r_rate = 1000
    s_rate = 1000
    r_window = 1000
    s_window = 1000
    batch = 100
    skew = 0

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "algorithm,inputTuples,time,throughput\n")
            commons.compile_app()
        for algorithm in algorithms:
            for size in sizes:
                expConfig = commons.Config(algorithm, batch, r_rate, s_rate, size, size, r_window, s_window, skew, 0)
                join(expConfig, config['repetitions'])

    if config['plot']:
        plot()