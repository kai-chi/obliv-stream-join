#!/usr/bin/python3
import os
import statistics
import subprocess

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt, ticker

import commons

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'
img_file = 'results/'  + filename + '.png'
batch_size = 0


def join(algorithm, size, batch, repetitions):
    print('Run self-join stream ' + algorithm + ' with size = ' + str(size) + ' and batch = ' + str(batch) + ' ' + str(repetitions) + ' times')
    joinTimes = []

    for i in range(repetitions):
        command = './app -a ' + algorithm + ' -r ' + str(size) + ' -s ' + str(size) + ' -g ' + str(batch)
        # stdout = ''
        try:
            stdout = subprocess.check_output(command, cwd='../', shell=True, stderr=subprocess.DEVNULL) \
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
    result = algorithm + ',' + str(size) + ',' + str(batch) + ',' + str(joinTime)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()

def plot():
    data = pd.read_csv(res_file)
    fig = plt.figure(figsize=(10,3))
    platforms = data['sgx'].unique()

    plots = ['initTime', 'readTime', 'writeTime']
    ylabels = ['Initialization Time(milliseconds)','Search Time(milliseconds)',
               'Insert Time(milliseconds)']
    for i in range(len(plots)):
        plt.subplot(1, 3, i+1)
        for algorithm in platforms:
            df = data[data['sgx'] == algorithm]
            sizes = df['size']
            times = df[plots[i]] / 1000
            plt.plot(sizes, times, label=algorithm, marker='o')

    # plt.title('Self-join stream comparison with batch=' + str(batch_size))
        plt.xlabel('OMAP Size')
        plt.ylabel(ylabels[i])
        # plt.xscale('log')
        plt.gca().set_xscale('log',basex=2)
        plt.yscale('log')
        plt.xticks(sizes[0::2])
        plt.legend()

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)


    if config['plot']:
        plot()