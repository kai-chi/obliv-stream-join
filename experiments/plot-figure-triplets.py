#!/usr/bin/python3
import os
import statistics
import subprocess

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import AutoMinorLocator

from commons import *

import yaml

window_res_file = 'results/window-size.csv'
batch_res_file = 'results/batch-size.csv'
scal_res_file = 'results/scalability.csv'


def plot_improvement():
    plt.rcParams.update(plt.rcParamsDefault)
    data = pd.read_csv('results/window-size.csv')
    plt.figure(figsize=(3,3))

    sizes = data[(data['algorithm'] == 'FK-MERG-L3') & (data['dataset'] == 'synth-1')]
    sizes = sizes['window']

    merg_l3 = data[(data['algorithm'] == 'FK-MERG-L3') & (data['dataset'] == 'synth-1')]
    sort_l3 = data[(data['algorithm'] == 'FK-SORT-L3') & (data['dataset'] == 'synth-1')]
    l3 = sort_l3['time']/merg_l3['time'].values
    plt.plot(sizes, l3, label='FK-MERG-L3 vs. FK-SORT-L3', marker='o')

    merg_l4 = data[(data['algorithm'] == 'FK-MERG-L4') & (data['dataset'] == 'synth-1')]
    sort_l4 = data[(data['algorithm'] == 'FK-SORT-L4') & (data['dataset'] == 'synth-1')]
    l4 = sort_l4['time']/merg_l4['time'].values
    plt.plot(sizes, l4, label='FK-MERG-L4 vs. FK-SORT-L4', marker='o')
    plt.xscale('log', base=10)
    plt.legend(fontsize='small')

    plt.xlabel('Window size [tuples]')
    plt.ylabel('Throughput improvement factor')
    plt.ylim(top=5, bottom=0)
    # plt.yscale('log', base=2)
    savefig('results/improvement.png', tight_layout=True)


def plot(leakage, algs, out_file, scalability=True,kras_join=False):
    plt.rcParams.update(plt.rcParamsDefault)
    data_window = pd.read_csv(window_res_file)
    data_batch = pd.read_csv(batch_res_file)
    data_scal = pd.read_csv(scal_res_file)


    plt.figure(figsize=(6,3))

    # WINDOW SUBPLOT
    plt.subplot(1, 2, 1)
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'a) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 2, 2)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'b) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    #SCALABILITY SUBPLOT
    if scalability:
        plt.subplot(1, 3, 3)
        data = data_scal[data_scal['algorithm'].isin(algs)]
        algorithms = data['algorithm'].unique()

        for algorithm in algorithms:
            df = data[data['algorithm'] == algorithm]
            threads = df['threads']

            # throughputs = (cfg.r_size+cfg.s_size)/df['time']
            times = df['time']
            times = times.iloc[0] / times
            # times = times.div(times.iloc[0])
            plt.plot(threads, times, label=algorithm, marker='o',
                     color=color_categorical(algorithm))
        # plt.legend(fontsize='x-small')
        plt.xlabel('threads')
        plt.xscale('log', base=2)
        plt.yscale('log', base=2)
        # plt.ylim(bottom=25, top= 32)
        # plt.xlim(right=16)
        # if i == 0:
        plt.ylabel('Speedup factor')
        plt.title(r'c) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ scalability.')

    savefig(out_file, tight_layout=True)


def plot_l2_l3_l2(kras_join=False):
    out_file = 'results/combined2.png'
    FK_L2 = ['FK-EPHI-L2', 'SHJ-L2', 'NLJ-L4', 'SHJ', 'FK-SORT-L2', 'FK-MERG-L2']
    NFK_L3 = ['NFK-JOIN-L3', 'NLJ-L4', 'SHJ','FK-MERG-L3']
    NFK_L2 = ['NFK-JOIN-L2', 'NLJ-L4', 'SHJ','FK-EPHI-L2', 'SHJ-L2']
    yticks = [0.01,10,1000,10000]

    plt.rcParams.update(plt.rcParamsDefault)
    data_window = pd.read_csv(window_res_file)
    data_batch = pd.read_csv(batch_res_file)
    data_scal = pd.read_csv(scal_res_file)


    plt.figure(figsize=(18,3.5))

#############################################################################################
    # WINDOW SUBPLOT
    plt.subplot(1, 6, 1)
    algs = FK_L2
    leakage = 'L2'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    ax.set_ylim(bottom=0.01, top=10000)
    ax.set_yticks(yticks)
    plt.yscale('log')
    plt.xscale('log')
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'a) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 6, 2)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'b) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    ##222222###########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(1, 6, 3)
    algs = NFK_L3
    leakage = 'L3'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    ax.set_yticks(yticks)
    ax.set_ylim(bottom=0.01, top=10000)
    # if i == 0:
    # plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'c) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 6, 4)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'd) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    ###333333333333333##########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(1, 6, 5)
    algs = NFK_L2
    leakage = 'L2'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    ax.set_yticks(yticks)
    ax.set_ylim(bottom=0.01, top=10000)
    # if i == 0:
    # plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'e) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 6, 6)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'f) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    savefig(out_file, tight_layout=True)

def plot_fk_joins(kras_join=False):
    fs=12
    legend_fs='medium'
    ytop=30000
    out_file = 'results/combined_fk_joins.png'
    FK_L2 = ['FK-EPHI-L2', 'NLJ-L4', 'SHJ', 'FK-SORT-L2', 'FK-MERG-L2']
    FK_L3 = ['NLJ-L4', 'SHJ', 'FK-SORT-L3', 'FK-MERG-L3']
    FK_L4 = ['NLJ-L4', 'SHJ', 'FK-SORT-L4', 'FK-MERG-L4']
    yticks = [0.01,1,100,10000]

    # FK_L2 = ['FK-EPHI-L2', 'NLJ-L4', 'SHJ', 'FK-SORT-L2', 'FK-MERG-L2',
    #          'FK-SORT-L3', 'FK-MERG-L3', 'FK-SORT-L4', 'FK-MERG-L4']

    plt.rcParams.update(plt.rcParamsDefault)
    data_window = pd.read_csv(window_res_file)
    data_batch = pd.read_csv(batch_res_file)
    data_scal = pd.read_csv(scal_res_file)


    plt.figure(figsize=(18,3.5))

    #############################################################################################
    # WINDOW SUBPLOT
    plt.subplot(1, 6, 3)
    algs = FK_L2
    leakage = 'L2'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    plt.legend(fontsize=legend_fs)
    handles, labels = plt.gca().get_legend_handles_labels()
    # if leakage == 'L4':
    #     order = [0,4,1,2,3]
    # elif leakage == 'L3':
    #     if len(labels) == 5:
    #         order = [0,4,2,1,3]
    #     else:
    #         order = [0,2,3,1]
    # elif leakage == 'L2':
    #     if len(labels) == 5:
    #         order = [0,2,1,4,3]
    #     else:
    #         order = [0,4,1,3,2,5]
    # else:
    #     order=[]

    # plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    plt.xlabel('Window size [tuples]', fontsize=fs)
    plt.yscale('log')
    plt.xscale('log')
    ax.set_ylim(bottom=0.01, top=ytop)
    ax.set_yticks(yticks)
    plt.xticks(fontsize=fs)
    plt.yticks(fontsize=fs)

    # if i == 0:

    # plt.minorticks_on()
    plt.title(r'c) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.', fontsize=fs)
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 6, 6)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            # draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            # plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            # draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            # plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=ytop, bottom=0.01)
    plt.xlabel('Batch size [tuples]', fontsize=fs)
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')
    # ax.set_yticks(yticks)
    plt.xticks(fontsize=fs)
    plt.yticks(yticks, fontsize=fs)

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    else:
        plt.legend(fontsize=legend_fs)
    plt.title(r'f) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.', fontsize=fs)
    plt.gca().minorticks_off()

    ##222222###########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(1, 6, 2)
    algs = FK_L3
    leakage = 'L3'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    plt.legend(fontsize=legend_fs)
    handles, labels = plt.gca().get_legend_handles_labels()
    # if leakage == 'L4':
    #     order = [0,4,1,2,3]
    # elif leakage == 'L3':
    #     if len(labels) == 5:
    #         order = [0,4,2,1,3]
    #     else:
    #         order = [0,2,3,1]
    # elif leakage == 'L2':
    #     if len(labels) == 5:
    #         order = [0,2,1,4,3]
    #     else:
    #         order = [0,4,1,3,2,5]
    # else:
    #     order=[]
    #
    # plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    plt.xlabel('Window size [tuples]', fontsize=fs)
    plt.yscale('log')
    plt.xscale('log')
    ax.set_yticks(yticks)
    plt.xticks(fontsize=fs)
    plt.yticks(fontsize=fs)
    ax.set_ylim(bottom=0.01, top=ytop)
    # if i == 0:
    # plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'b) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.', fontsize=fs)
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 6, 5)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    # if baseline == 'SHJ-L2':
    #     if kras_join:
    #         draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
    #         plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
    #         draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
    #         plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
    #         draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
    #         plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    #     else:
    #         draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
    #         plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
    #         draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
    #         plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    # elif baseline == 'SHJ-L3':
    #     # draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
    #     # plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    # elif baseline == 'SHJ-L4':
    #     draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
    #     plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=ytop, bottom=0.01)
    plt.xlabel('Batch size [tuples]', fontsize=fs)
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')
    plt.xticks(fontsize=fs)
    plt.yticks(yticks, fontsize=fs)
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    else:
        plt.legend(fontsize=legend_fs)
    plt.title(r'e) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.', fontsize=fs)
    plt.gca().minorticks_off()

    ###333333333333333##########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(1, 6, 1)
    algs = FK_L4
    leakage = 'L4'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    plt.legend(fontsize=legend_fs)
    handles, labels = plt.gca().get_legend_handles_labels()
    # if leakage == 'L4':
    #     order = [0,4,1,2,3]
    # elif leakage == 'L3':
    #     if len(labels) == 5:
    #         order = [0,4,2,1,3]
    #     else:
    #         order = [0,2,3,1]
    # elif leakage == 'L2':
    #     if len(labels) == 5:
    #         order = [0,2,1,4,3]
    #     else:
    #         order = [0,4,1,3,2,5]
    # else:
    #     order=[]
    #
    # plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    plt.xlabel('Window size [tuples]', fontsize=fs)
    plt.yscale('log')
    plt.xscale('log')
    ax.set_yticks(yticks)
    plt.xticks(fontsize=fs)
    plt.yticks(fontsize=fs)
    ax.set_ylim(bottom=0.01, top=ytop)
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]', fontsize=fs)

    # plt.minorticks_on()
    plt.title(r'a) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.', fontsize=fs)
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(1, 6, 4)
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=ytop, bottom=0.01)
    plt.xlabel('Batch size [tuples]', fontsize=fs)
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')
    plt.xticks(fontsize=fs)
    plt.yticks(yticks, fontsize=fs)

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    else:
        plt.legend(fontsize=legend_fs)
    plt.title(r'd) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.', fontsize=fs)
    plt.gca().minorticks_off()
    plt.rcParams.update({'font.size': 15})
    plt.tight_layout(pad=0.2)
    plt.savefig(out_file, transparent = False, bbox_inches = 'tight', pad_inches = 0.1, dpi=200)
    print("Saved image file: " + out_file)


def plot_l4_l3(kras_join=False):
    out_file = 'results/combined34.png'
    FK_L3 = ['FK-MERG-L3', 'SHJ-L3', 'NLJ-L4', 'SHJ', 'FK-SORT-L3']
    FK_L4 = ['FK-MERG-L4', 'SHJ-L4', 'NLJ-L4', 'SHJ', 'FK-SORT-L4', 'FK-MERG-L4-SPLIT']

    plt.rcParams.update(plt.rcParamsDefault)
    data_window = pd.read_csv(window_res_file)
    data_batch = pd.read_csv(batch_res_file)
    data_scal = pd.read_csv(scal_res_file)


    plt.figure(figsize=(6,6))
    # plt.subplots_adjust(hspace=0.1)  # Default is usually 0.2-0.4

    #############################################################################################
    # WINDOW SUBPLOT
    plt.subplot(2, 2, 1)
    algs = FK_L4
    leakage = 'L4'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    plt.ylim(top=20000, bottom=0.0003)
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'c) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(2, 2, 2)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'f) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    ##222222###########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(2, 2, 3)
    algs = FK_L3
    leakage = 'L3'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    plt.ylim(top=20000, bottom=0.0003)
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'b) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(2, 2, 4)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'e) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    savefig(out_file, tight_layout=True)


def plot_nonfk(kras_join=True):
    out_file = 'results/combined_nfk.png'

    NFK_L3 = ['NFK-JOIN-L3', 'NLJ-L4', 'SHJ','FK-MERG-L3']
    NFK_L2 = ['NFK-JOIN-L2', 'NLJ-L4', 'SHJ','FK-EPHI-L2', 'SHJ-L2']

    plt.rcParams.update(plt.rcParamsDefault)
    data_window = pd.read_csv(window_res_file)
    data_batch = pd.read_csv(batch_res_file)
    data_scal = pd.read_csv(scal_res_file)


    plt.figure(figsize=(6,6))
    # plt.subplots_adjust(hspace=0.1)  # Default is usually 0.2-0.4

    #############################################################################################
    # WINDOW SUBPLOT
    plt.subplot(2, 2, 1)
    algs = NFK_L3
    leakage = 'L3'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    plt.ylim(top=20000, bottom=0.0003)
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'c) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(2, 2, 2)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'f) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    ##222222###########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(2, 2, 3)
    algs = NFK_L2
    leakage = 'L2'
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    # plt.axes().yaxis.set_minor_locator(AutoMinorLocator())
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        # throughputs = (cfg.r_size+cfg.s_size)/df['time']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='x-small')
    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [0,4,1,2,3]
    elif leakage == 'L3':
        if len(labels) == 5:
            order = [0,4,2,1,3]
        else:
            order = [0,2,3,1]
    elif leakage == 'L2':
        if len(labels) == 5:
            order = [0,2,1,4,3]
        else:
            order = [0,4,1,3,2,5]
    else:
        order=[]

    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    plt.xlabel('Window size [tuples]')
    plt.yscale('log')
    plt.xscale('log')
    plt.ylim(top=20000, bottom=0.0003)
    # if i == 0:
    plt.ylabel('Throughput [K tuples / s]')

    # plt.minorticks_on()
    plt.title(r'b) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ window size.')
    # ax.minorticks_on()

    # ax.xaxis.set_minor_locator(plt.AutoMinorLocator())
    # ax.grid(True, which='major', linestyle='-', alpha=0.7)
    # ax.grid(True, which='minor', linestyle=':', alpha=0.4)

    ########## BATCH SUBPLOT ##############
    plt.subplot(2, 2, 4)
    shj_l2_throughput = 7.16e-6 # M rec/s
    shj_l3_throughput = 8.764e-6 # M rec/s
    shj_l4_throughput = 0.377e-6 # M rec/s
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    opaq_l2_throughput = 41.929e-6 # M rec / s
    join_l2_throughput = 15.6e-6 # M rec/s
    baseline = 'SHJ-' + leakage

    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

    opaq_l4 = []
    oca_l4 = []
    for alg in algorithms:
        d = data[data['algorithm'] == alg]
        batches = d['batch']
        thr = 1000*d['throughput']
        if alg == 'FK-SORT-L4':
            opaq_l4 = thr
        elif alg == 'FK-MERG-L4-SPLIT':
            oca_l4 = thr
        plt.plot(batches, thr, color=color_categorical(alg), label=alg, marker='o')

    # impr = np.array(oca_l4) / np.array(opaq_l4)
    # print("FK-MERG-L4/FK-SORT-L4       (avg=" + str(round(np.mean(impr),2)) + "): " + str(impr))
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'))
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4')
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'))
    plt.text(100, 1000*shj_throughput*1.2, 'SHJ')
    if baseline == 'SHJ-L2':
        if kras_join:
            draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'))
            plt.text(10000, 1000*join_l2_throughput*0.8, 'NFK-JOIN-L2')
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*0.25, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
        else:
            draw_horizontal_line(plt, 1000*shj_l2_throughput, color=color_categorical(baseline))
            plt.text(100, 1000*shj_l2_throughput*1.2, baseline)
            draw_horizontal_line(plt, 1000*opaq_l2_throughput, color=color_categorical('FK-SORT-L2'))
            plt.text(100, 1000*opaq_l2_throughput*1.2, 'FK-SORT-L2')
    elif baseline == 'SHJ-L3':
        draw_horizontal_line(plt, 1000*shj_l3_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l3_throughput*1.2, baseline)
    elif baseline == 'SHJ-L4':
        draw_horizontal_line(plt, 1000*shj_l4_throughput, color=color_categorical(baseline))
        plt.text(100, 1000*shj_l4_throughput*1.2, baseline)

    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=50000, bottom=0.0001)
    plt.xlabel('Batch size [tuples]')
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')

    handles, labels = plt.gca().get_legend_handles_labels()
    if leakage == 'L4':
        order = [1,0]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    elif leakage == 'L3' and len(labels)==2:
        order = [1,0]
        if kras_join:
            order=[0,1]
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='x-small')
    else:
        plt.legend(fontsize='x-small')
    plt.title(r'd) $\mathcal{' + leakage[0] + '}' + '_' + leakage[1] +'$ batch size.')
    plt.gca().minorticks_off()

    savefig(out_file, tight_layout=True)


if __name__ == '__main__':
    plot('L2', ['FK-EPHI-L2', 'SHJ-L2', 'NLJ-L4', 'SHJ', 'FK-SORT-L2', 'FK-MERG-L2'], 'results/l2-all-plots.png',scalability=False)
    # plot('L3', ['FK-MERG-L3', 'SHJ-L3', 'NLJ-L4', 'SHJ', 'FK-SORT-L3'], 'results/l3-all-plots.png',scalability=False)
    # plot('L4', ['FK-MERG-L4', 'SHJ-L4', 'NLJ-L4', 'SHJ', 'FK-SORT-L4', 'FK-MERG-L4-SPLIT'], 'results/l4-all-plots.png',scalability=False)
    # plot('L3', ['NFK-JOIN-L3', 'NLJ-L4', 'SHJ','FK-MERG-L3'], 'results/l3-kras.png', scalability=False, kras_join=True)
    # plot('L2', ['NFK-JOIN-L2', 'NLJ-L4', 'SHJ','FK-EPHI-L2', 'SHJ-L2'], 'results/l2-kras.png', scalability=False, kras_join=True)
    # plot_improvement()
    # plot_l2_l3_l2()
    # plot_l4_l3()
    # plot_nonfk()
    plot_fk_joins()