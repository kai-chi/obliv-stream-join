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


def join(algorithm, size, repetitions):
    print('Run self-join ' + algorithm + ' with size = ' + str(size) + ' ' + str(repetitions) + ' times')
    leftInitTimes = []
    leftBuildTimes = []
    leftProbeTimes = []
    rightInitTimes = []
    rightBuildTimes = []
    rightProbeTimes = []
    joinTotalTimes = []

    for i in range(repetitions):
        command = './app -a ' + algorithm + ' -r ' + str(size) + ' -s ' + str(size) + ' --self-join '
        # stdout = ''
        try:
            stdout = subprocess.check_output(command, cwd='../', shell=True, stderr=subprocess.DEVNULL) \
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            print("App error:\n", e.output)
            print(e.stdout)
            exit()
        print(stdout)
        for line in stdout.splitlines():
            if 'leftInitTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                leftInitTimes.append(time)
            elif 'leftBuildTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                leftBuildTimes.append(time)
            elif 'leftProbeTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                leftProbeTimes.append(time)
            elif 'rightInitTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                rightInitTimes.append(time)
            elif 'rightBuildTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                rightBuildTimes.append(time)
            elif 'rightProbeTime' in line:
                time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                rightProbeTimes.append(time)

    leftInitTime = statistics.median(leftInitTimes)
    leftBuildTime = statistics.median(leftBuildTimes)
    leftProbeTime = statistics.median(leftProbeTimes)
    rightInitTime = statistics.median(rightInitTimes)
    rightBuildTime = statistics.median(rightBuildTimes)
    rightProbeTime = statistics.median(rightProbeTimes)
    result = str(size) + ',' + str(leftInitTime) + ',' + str(leftBuildTime) + ',' + str(leftProbeTime) \
             + ',' + str(rightInitTime) + ',' + str(rightBuildTime) + ',' + str(rightProbeTime)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot():
    df = pd.read_csv(res_file)
    fig = plt.figure(figsize=(5,5))

    df.set_index('size').plot(kind='bar', stacked=True)

    plt.title('Self-join SHJ_Graphos split to phases')
    plt.xlabel('Input table size |R|')
    plt.ylabel('Execution time [micros]')
    plt.legend()

    commons.savefig(img_file)


if __name__ == '__main__':

    with open('../config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    sizes = 256 * (2 ** np.arange(11))

    if config['experiment']:
        if config['compile']:
            commons.remove_file(res_file)
            commons.init_file(res_file, "size,leftInitTime,leftBuildTime,leftProbeTime,rightInitTime,rightBuildTime,rightProbeTime\n")
            commons.compile_app()
        for size in sizes:
            join('SHJ_Graphos', size, config['repetitions'])

    if config['plot']:
        plot()