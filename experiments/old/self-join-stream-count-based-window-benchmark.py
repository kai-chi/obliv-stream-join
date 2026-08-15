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
batch_size = 0


def join(algorithm, size, batch, window, repetitions):
    print('Run self-join stream ' + str(algorithm) + ' with datasetSize=' + str(size) + ', batch=' + str(batch) +
          ', windowSize=' + str(window) + ' ' + str(repetitions) + ' times')
    joinTimes = []

    for i in range(repetitions):
        command = './app --alg ' + algorithm + ' -r ' + str(size) + ' -s ' + str(size) + ' --batch ' + str(batch) + \
                  ' --windowR ' + str(window) + ' --windowS ' + str(window)
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
    result = algorithm + ',' + str(size) + ',' + str(batch) + ',' + str(window) + ',' + str(window) + ',' + str(joinTime)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()

def plot():
    data = pd.read_csv(res_file)
    fig = plt.figure(figsize=(5,5))
    windows = data['windowR'].unique()

    for window in windows:
        df = data[data['windowR'] == window]
        sizes = df['sizeRelation']
        times = df['joinTotalTime'] / 1000000
        plt.plot(sizes, times, label=window, marker='o')

    plt.title('Self-join stream with batch=' + str(batch_size))
    plt.xlabel('Input table size |R|')
    plt.ylabel('Execution time [s]')
    plt.xscale('log')
    plt.yscale('log')
    plt.legend(title="Window Size")
    # commons.draw_horizontal_lines(plt, [3600*12])
    # plt.text(1000, 3600*13,'12h')

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algorithm = 'SHJ_Graphos'
    sizes = 512 * (2 ** np.arange(3))
    batch = 100
    batch_size = batch
    windows = [100, 1000, 10000]

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "algorithm,sizeRelation,batchSize,windowR,windowS,joinTotalTime\n")
            commons.compile_app()
        for window in windows:
            for size in sizes:
                join(algorithm, size, batch, window, config['repetitions'])

    if config['plot']:
        plot()