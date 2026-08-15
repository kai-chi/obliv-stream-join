#!/usr/bin/python3
import os
import subprocess


import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from statistics import median
import commons

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'
img_file = 'results/'  + filename + '.png'


def join(expConfig, repetitions):
    print('Run join stream ' + str(repetitions) + ' times with ' + str(expConfig))
    joinTimes = []
    throughputs = []
    buildTimes = []
    probeTimes = []
    deleteTimes = []

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
            elif 'buildTime' in line:
                buildTimes.append(float(commons.escape_ansi(line.split(": ", 1)[1])))
            elif 'probeTime' in line:
                probeTimes.append(float(commons.escape_ansi(line.split(": ", 1)[1])))
            elif 'deleteTime' in line:
                deleteTimes.append(float(commons.escape_ansi(line.split(": ", 1)[1])))

    bt = median(buildTimes)
    pt = median(probeTimes)
    dt = median(deleteTimes)
    jt = median(joinTimes)
    th = median(throughputs)

    result = (str(expConfig.nthreads) + ',' + str(expConfig.orams) + ',' +
              str(int(expConfig.orams/expConfig.nthreads)) + ','
              + str(expConfig.r_size+expConfig.s_size) + ',' +
              str(bt) + ',' + str(pt) + ',' + str(dt) + ',' +
              str(jt) + ',' + str(th))
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot():
    data = pd.read_csv(res_file)
    fig = plt.figure(figsize=(4,4))
    ax = fig.gca()
    ax.xaxis.get_major_locator().set_params(integer=True)

    orams_per_thread = data['oramsPerThread'].unique()
    for orams in orams_per_thread:
        df = data[data['oramsPerThread'] == orams]
        threads = df['nthreads']
        thr = 1000*df['inputTuples']/df['time']
        plt.plot(threads, thr, label=str(orams), marker='o')

    plt.title('J2r scalability')
    ax.set_xlabel('threads')
    plt.ylabel('Join throughput [K rec / s]')
    plt.ylim(bottom=0)

    # plt.xticks()
    plt.xscale('log', base=2)
    ax.set_xticks([1,2,4,8,16,32])
    plt.legend(title='ORAMs per thread', fontsize='small')

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    rate = 1000
    orams_per_thread = [1, 2, 3, 4]
    window = 5000
    size = 10000
    skew = 0

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "nthreads,orams,oramsPerThread,inputTuples,buildTime,probeTime,deleteTime,time,throughput\n")
            commons.compile_app()

        for oramspt in orams_per_thread:
            for i in range(5,-1,-1):
                threads = int(pow(2, i))
                orams = threads * oramspt
                expConfig = commons.Config(algorithm='J2r', r_rate=rate, s_rate=rate, r_size=size, s_size=size,
                                       r_window=window, s_window=window, skew=skew, nthreads=threads, orams=orams)
                join(expConfig, config['repetitions'])

    if config['plot']:
        plot()
