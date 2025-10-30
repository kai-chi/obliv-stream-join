#!/usr/bin/python3
import os
import statistics
import subprocess

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

import commons

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'
img_file = 'results/'  + filename + '.png'


def join(expConfig, repetitions):
    print('Run join stream ' + str(repetitions) + ' times with ' + str(expConfig))
    joinTimes = []

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

    joinTime = statistics.median(joinTimes)
    result = algorithm + ',' + str(expConfig.r_size+expConfig.s_size) + ',' + str(joinTime) + ',' + str(expConfig.skew)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(cfg: commons.Config):
    data = pd.read_csv(res_file)
    plt.figure(figsize=(5,3.5))
    algorithms = data['algorithm'].unique()

    # plt.bar(data['algorithm'], data['inputTuples']/data['time'], tick_label=algorithms)

    throughputs = data['inputTuples']/data['time']
    data.insert(4, 'throughput', throughputs, True)
    # data.set_index('algorithm').plot(kind='bar',y='throughput')

    sns.barplot(x='algorithm', hue='skew', y='throughput', data=data)

    plt.title('Stream join throughput with batch=' + str(cfg.batch) + ',\n total input size=' +
              str(cfg.r_size+cfg.s_size) + ', windows size=' + str(cfg.r_window))
    plt.xlabel('Algorithm')
    plt.ylabel('Throughput [M rec / s]')
    plt.yscale('log')
    plt.legend(title='skew', fontsize='small')
    # commons.draw_horizontal_lines(plt, [3600*12])
    # plt.text(1000, 3600*13,'12h')

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algorithms = ['OPAQUE', 'J0', 'J3_FK', 'J4']
    r_rate = 1000
    s_rate = 1000
    r_size = 1000000
    s_size = 1000000
    r_window = 1000
    s_window = 1000
    batch = 100
    skews = [0, 0.99]
    cfg = commons.Config(r_rate=1000, s_rate=1000, r_size=1000000, s_size=1000000,
                         r_window=1000, s_window=1000, batch=100, skew=0, fk_join=True)

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "algorithm,inputTuples,time,skew\n")
            commons.compile_app()
        for algorithm in algorithms:
            for skew in skews:
                expConfig = commons.Config(algorithm, batch, r_rate, s_rate, r_size, s_size, r_window, s_window, skew, True)
                join(expConfig, config['repetitions'])

    if config['plot']:
        plot(cfg)