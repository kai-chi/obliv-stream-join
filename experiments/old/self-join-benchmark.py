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


def join(algorithm, size, repetitions):
    print('Run self-join ' + algorithm + ' with size = ' + str(size) + ' ' + str(repetitions) + ' times')
    joinTimes = []

    for i in range(repetitions):
        command = './app -a ' + algorithm + ' -r ' + str(size) + ' -s ' + str(size) + ' --self-join '
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
    result = algorithm + ',' + str(size) + ',' + str(size) + ',' + str(joinTime)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()

def plot():
    data = pd.read_csv(res_file)
    fig = plt.figure(figsize=(5,5))
    algorithms = data['algorithm'].unique()

    for algorithm in algorithms:
        df = data[data['algorithm'] == algorithm]
        sizes = df['sizeR']
        times = df['time']
        plt.plot(sizes, times, label=algorithm, marker='o')

    plt.title('Self-join comparison')
    plt.xlabel('Input table size |R|')
    plt.ylabel('Execution time [micros]')
    plt.xscale('log')
    plt.yscale('log')
    plt.legend()

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algorithms = ['SHJ_NO', 'SHJ_Graphos']
    sizes = 256 * (2 ** np.arange(11))  # 256 - 262144

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "algorithm,sizeR,sizeS,time\n")
            commons.compile_app()
        for algorithm in algorithms:
            for size in sizes:
                join(algorithm, size, config['repetitions'])

    if config['plot']:
        plot()