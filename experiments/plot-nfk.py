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

def plot_nfk_joins(kras_join=False):
    fs=12
    out_file = 'results/nfk-joins2.png'
    shj = ['SHJ', 'SHJ-L0','SHJ-L1','SHJ-L2','SHJ-L3','SHJ-L4']
    nfks = ['SHJ', 'NLJ-L4', 'NFK-JOIN-L2','NFK-JOIN-L3']
    yticks = [0.0001,0.01,1,100,10000]
    ytop = 20000
    ymin = 0.0001
    rows = 2
    columns = 2
    lw=1.3
    lp = -0.3
    ytitle=0.991

    plt.rcParams.update(plt.rcParamsDefault)
    data_window = pd.read_csv(window_res_file)
    data_batch = pd.read_csv(batch_res_file)


    plt.figure(figsize=(6,5.2))

    #############################################################################################
    # WINDOW SUBPLOT
    plt.subplot(rows, columns, 3)
    algs = shj
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='small')
    handles, labels = plt.gca().get_legend_handles_labels()
    order=[0,5,4,2,1,3]
    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    plt.ylabel('Throughput [K tuples / s]', fontsize=fs)
    plt.xlabel('Window size [tuples]', fontsize=fs, labelpad=lp)
    plt.yscale('log')
    plt.xscale('log')
    ax.set_ylim(bottom=ymin, top=ytop)
    ax.set_yticks(yticks)
    plt.xticks(fontsize=fs-1)
    plt.yticks(fontsize=fs-1)
    plt.title(r'c) SHJ family window size.', fontsize=fs, y=ytitle)


    ##2222222#########################################################################################
    # WINDOW SUBPLOT
    plt.subplot(rows, columns, 1)
    algs = nfks
    ax = plt.gca()
    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()
    data2 = data_window[data_window['algorithm'].isin(algs)]
    algorithms = data2['algorithm'].unique()

    for algorithm in algorithms:
        df = data2[(data2['algorithm'] == algorithm) & (data2['dataset'] == 'synth-1')]
        sizes = df['window']
        throughputs = 1000*df['input-tuples']/df['time']
        plt.plot(sizes, throughputs, label=algorithm, marker='o',
                 color=color_categorical(algorithm))
    # plt.legend(fontsize='small')
    handles, labels = plt.gca().get_legend_handles_labels()
    order=[0,2,1,3]
    plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
    plt.xlabel('Window size [tuples]', fontsize=fs, labelpad=lp)
    plt.yscale('log')
    plt.xscale('log')
    ax.set_ylim(bottom=ymin, top=ytop)
    ax.set_yticks(yticks)
    plt.xticks(fontsize=fs-1)
    plt.yticks(fontsize=fs-1)
    plt.title(r'a) Non-FK stream joins.', fontsize=fs, y=ytitle)


    ########## BATCH SUBPLOT ##############
    plt.subplot(rows, columns, 2)
    nlj_throughput = 1461.066e-6 # M rec/s
    shj_throughput = 6.2561 # M rec/s
    join_l2_throughput = 15.6e-6 # M rec/s

    algs = nfks
    data = data_batch

    data = data[data['algorithm'].isin(algs)]
    algorithms = data['algorithm'].unique()

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
    draw_horizontal_line(plt, 1000*nlj_throughput, color=color_categorical('NLJ-L4'), linewidth=lw)
    plt.text(15000, 1000*nlj_throughput*1.2, 'NLJ-L4', fontsize=fs-1)
    draw_horizontal_line(plt, 1000*shj_throughput, color=color_categorical('SHJ'), linewidth=lw)
    plt.text(18000, 1000*shj_throughput*1.2, 'SHJ', fontsize=fs-1)
    draw_horizontal_line(plt, 1000*join_l2_throughput, color=color_categorical('NFK-JOIN-L2'), linewidth=lw)
    plt.text(4500, 1000*join_l2_throughput*1.2, 'NFK-JOIN-L2', fontsize=fs-1)


    # plt.ylim(top=50000, bottom=0.01)
    plt.ylim(top=ytop*1.1, bottom=0.01)
    plt.ylabel('Throughput [K tuples / s]', fontsize=fs)
    plt.xlabel('Batch size [tuples]', fontsize=fs, labelpad=lp)
    # plt.ylabel('Throughput\n[K tuples / s]')
    plt.xscale('log')
    plt.yscale('log')
    # ax.set_yticks(yticks)
    plt.xticks(fontsize=fs-1)
    plt.yticks([0.01,1,100,10000], fontsize=fs-1)

    plt.legend(fontsize='small')
    plt.title(r'b) Non-FK stream joins.', fontsize=fs, y=ytitle)
    ax = plt.gca()

    ax.yaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.xaxis.set_minor_locator(plt.LogLocator(subs='all'))
    ax.minorticks_off()

    ####################################################################################################
    data = pd.read_csv('results/oblivious-merge.csv')
    plt.subplot(rows, columns, 4)
    # ax = fig.gca()
    # ax.xaxis.get_major_locator().set_params(integer=True)

    batches = data['batch-size'].unique()

    for i in range(2,len(batches)):
        ax = plt.gca()
        df = data[data['batch-size'] == batches[i]]
        sizes = df['window-size'].unique()
        oddEvenMerge = df[df['alg'] == 1]['mean']
        oddEvenMergeOpt = df[df['alg'] == 2]['mean']
        bitonicSort = df[df['alg'] == 3]['mean']
        bitonicMerge = df[df['alg'] == 7]['mean']
        # oddEvenMergePar = df[df['alg'] == 4]['mean']/1000
        # improvementMerge = oddEvenMerge.to_numpy()/oddEvenMergeOpt.to_numpy()
        # improvementSort = bitonicSort.to_numpy()/oddEvenMergeOpt.to_numpy()
        # improvementPar = oddEvenMergePar.to_numpy()/oddEvenMergeOpt.to_numpy()
        # plt.axvline(x=3413, linestyle='dotted', color='gray', linewidth=1) # L1
        # plt.axvline(x=87381, linestyle='dotted', color='gray', linewidth=1) # L2
        # plt.axvline(x=1638400, linestyle='dotted', color='gray', linewidth=1) # L3

        # ax2 = ax.twinx()

        ### results ###

        # plt.plot(sizes, oddEvenMerge, label='oddEvenMerge', marker='x')
        plt.plot(sizes, oddEvenMergeOpt, label='oddEvenMergeOpt', marker='o')
        plt.plot(sizes, bitonicSort, label='bitonicSort', marker='^')
        plt.plot(sizes, bitonicMerge, label='bitonicMerge', marker='x')
        # plt.plot(sizes, oddEvenMergePar, label='oddEvenMergePar', marker='*')
        if i == 2:
            ax.set_ylabel('Merge/Sort time [ms]', fontsize=fs)
            handles, labels = plt.gca().get_legend_handles_labels()
            order = [1,0,2]
            plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order], fontsize='small')
        plt.yscale('log', base=2)

        ### improvement ###
        # ax.plot(sizes, improvementMerge, linestyle='dashed', color='gray', marker='x', label='over merge')
        # ax.plot(sizes, improvementSort, linestyle='dashed', color='gray', marker='^', label='over sort')
        # ax.axhline(y=1, linestyle='--', color='green', linewidth=1)
        # ax.plot(sizes, improvementPar, linestyle='dashed', color='gray', marker='*', label='over parallel')
        # ax.set_ylim([0, 18])
        # if i == 0:
        #     ax.set_ylabel('Improvement factor')
        #     ax.legend(title='Improvement:', fontsize='x-small')


        # ax.set_title('a) Performance\n(batch=$2^{10}$)')
        # ax.set_title('d) Oblivious primitives.', fontsize=fs, y=ytitle)
        plt.title(r'd) Oblivious primitives comparison.', fontsize=fs, y=ytitle)
        plt.xlim(left=2**10, right=2**23)
        plt.xscale('log', base=2)
        # ax.set_yticks([1,2,4,8,16,32,64,128,256])
        # ax.set_xlabel('Array size [tuples]', fontsize=fs)
        plt.xlabel('Array size [tuples]', fontsize=fs, labelpad=lp)
        powers_of_two = [2**i for i in range(10, 24,3)]
        ax.set_xticks(powers_of_two)
        powers_of_two = [2**i for i in range(-4, 12,3)]
        ax.set_yticks(powers_of_two)
        ax.tick_params(axis='x', labelsize=fs-1)
        ax.tick_params(axis='y', labelsize=fs-1)

    plt.gca().minorticks_off()
    plt.rcParams.update({'font.size': 15})
    plt.tight_layout(pad=0.2)
    plt.savefig(out_file, transparent = False, bbox_inches = 'tight', pad_inches = 0.1, dpi=200)
    print("Saved image file: " + out_file)


if __name__ == '__main__':
    plot_nfk_joins()