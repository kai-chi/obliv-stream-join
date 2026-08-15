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
        print('Command: ' + expConfig.command())

        # stdout = ''
        try:
            stdout = subprocess.check_output(expConfig.command(), cwd='../', shell=True, stderr=subprocess.DEVNULL) \
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
    result = str(expConfig.alpha) + ',' + str(expConfig.r_size+expConfig.s_size) + ',' + str(joinTime) + ',' + str(thr)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot():
    data = pd.read_csv(res_file)
    fig = plt.figure(figsize=(3,3))
    ax = fig.gca()
    ax.xaxis.get_major_locator().set_params(integer=True)
    alphas = data['alpha']
    thr = 1000*data['inputTuples']/data['time']
    plt.plot(alphas, thr, label='J2a', marker='o')

    plt.title('J2a trade-off')
    # plt.xlabel(r'$\alpha')
    ax.set_xlabel('α')
    plt.ylabel('Join throughput [K rec / s]')
    plt.ylim(bottom=0)
    # plt.xscale('log')
    # plt.yscale('log')

    commons.draw_horizontal_lines(plt, [16384000/261583217])
    plt.text(12, 0.12,'J2')
    plt.legend()

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    r_rate = 1000
    s_rate = 1000
    r_window = 8192
    s_window = 8192
    size = 8192
    skew = 0

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "alpha,inputTuples,time,throughput\n")
            commons.compile_app()
            for alpha in range(17):
                expConfig = commons.Config(algorithm='J2a', r_rate=r_rate, s_rate=s_rate, r_size=size, s_size=size,
                                           r_window=r_window, s_window=s_window, skew=skew, alpha=alpha)
                join(expConfig, config['repetitions'])

    if config['plot']:
        plot()
