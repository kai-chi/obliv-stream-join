#!/usr/bin/python3
import os
import statistics
import subprocess

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from matplotlib.ticker import AutoMinorLocator, LogLocator, NullFormatter

from commons import *

import yaml

filename = os.path.basename(__file__)[:-3]
res_file = 'results/' + filename + '.csv'
img_file = 'results/'  + filename + '.png'


def join(cfg: Config, repetitions):
    print('Run join stream ' + str(repetitions) + ' times with ' + str(cfg))
    joinTimes = []

    for i in range(repetitions):
        print('Command: ' + cfg.command())

        # stdout = ''
        try:
            stdout = subprocess.check_output(cfg.command(), cwd='../', shell=True, stderr=subprocess.DEVNULL) \
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            print ("App error:\n", e.output)
            print (e.stdout)
            exit()
        print(stdout)
        for line in stdout.splitlines():
            if 'joinTotalTime' in line:
                time = int(escape_ansi(line.split(": ", 1)[1]))
                joinTimes.append(time)

    joinTime = statistics.median(joinTimes)
    if cfg.dataset == 'tpch-1':
        cfg.r_size = 150000
        cfg.s_size = 1500000

    result = algorithm + ',' + str(cfg.name) + ',' + str(cfg.r_size + cfg.s_size - cfg.r_window - cfg.s_window) + ',' + str(joinTime)
    print('Join results: ' + result)
    f = open(res_file, 'a')
    f.write(result + '\n')
    f.close()


def plot(algs, out_name):
    plt.rcParams.update(plt.rcParamsDefault)
    data = pd.read_csv(res_file)
    plt.figure(figsize=(10,5))
    data = data[data['algorithm'].isin(algs)]
    fs = 14

    hatches = ['-', 'o', '']

    # plt.bar(data['algorithm'], data['inputTuples']/data['time'], tick_label=algorithms)

    throughputs = 1000*data['inputTuples']/data['time']
    data.insert(4, 'throughput', throughputs, True)
    # data.set_index('algorithm').plot(kind='bar',y='throughput')

    # plt.gca().axvspan(2.5, 4.5, facecolor='#f0f0f0', alpha=1)
    # plt.gca().axvspan(4.5, 6.5, facecolor='#eaf8e0', alpha=1)
    # plt.gca().axvspan(6.5, 8.5, facecolor='#fafaf5', alpha=1)
    # plt.gca().axvspan(8.5, 11.5, facecolor='#f0f0f0', alpha=1)
    # plt.gca().axvspan(11.5, 13.5, facecolor='#eaf8e0', alpha=1)
    # plt.gca().axvspan(13.5, 15.5, facecolor='#fafaf5', alpha=1)
    l2_color = '#E5E4E2'
    l3_color = '#848884'
    l4_color = '#8A9A5B'
    alfa = 0.2
    plt.gca().axvspan(2.5, 4.5, facecolor=l2_color, alpha=alfa)
    plt.gca().axvspan(4.5, 6.5, facecolor=l3_color, alpha=alfa)
    plt.gca().axvspan(6.5, 8.5, facecolor=l4_color, alpha=alfa)
    plt.gca().axvspan(8.5, 11.5, facecolor=l2_color, alpha=alfa)
    plt.gca().axvspan(11.5, 13.5, facecolor=l3_color, alpha=alfa)
    plt.gca().axvspan(13.5, 15.5, facecolor=l4_color, alpha=alfa)
    plt.gca().axvline(x=2.5, linestyle='--', color='black', linewidth=0.75)
    plt.gca().axvline(x=4.5, linestyle='--', color='black', linewidth=0.75)
    plt.gca().axvline(x=6.5, linestyle='--', color='black', linewidth=0.75)
    plt.gca().axvline(x=8.5, linestyle='-', color='black', linewidth=0.75)
    plt.gca().axvline(x=11.5, linestyle='--', color='black', linewidth=0.75)
    plt.gca().axvline(x=13.5, linestyle='--', color='black', linewidth=0.75)

    plt.text(3.25, 1000,'$\mathcal{L}_2$', fontsize=18)
    plt.text(5.35, 1000,'$\mathcal{L}_3$', fontsize=18)
    plt.text(7.35, 1000,'$\mathcal{L}_4$', fontsize=18)
    plt.text(9.8, 1000,'$\mathcal{L}_2$', fontsize=18)
    plt.text(12.35, 1000,'$\mathcal{L}_3$', fontsize=18)
    plt.text(14.35, 1000,'$\mathcal{L}_4$', fontsize=18)

    ax = sns.barplot(x='algorithm', hue='dataset', y='throughput', data=data, order=algs,
                     palette=["#fd7f6f", "#7eb0d5", "#b2e061"])

    palette = [color_categorical(x) for x in algs]
    for bars, hatch, legend_handle in zip(ax.containers, hatches, ax.legend_.legend_handles):
        for bar, color in zip(bars, palette):
            # bar.set_facecolor(color)
            bar.set_edgecolor('black')
            # bar.set_hatch(hatch)
        # legend_handle.set_hatch(hatch + hatch)

    plt.xlabel('Algorithm', fontsize=fs)
    plt.ylabel('Throughput [K rec / s]', fontsize=fs)
    # plt.yscale('log')

    plt.yscale('log')
    plt.yticks([0.001,0.1,10,1000], fontsize=fs)
    # ax.yaxis.set_minor_locator(LogLocator(subs=np.arange(2, 10)))
    # ax.yaxis.set_minor_formatter(NullFormatter())
    # ax.tick_params(axis='y', which='minor', length=4, width=1, color='gray')

    # plt.rc('xtick', labelsize=10)
    ax.tick_params(axis='x', labelsize=10)
    plt.xticks(rotation=30, fontsize=11)
    # plt.yticks(minor=False)
    # legend = plt.legend(title='dataset', fontsize='small', bbox_to_anchor=(0.2, 0.8), loc='upper left',
    #                     framealpha=1)
    legend = plt.legend(title='dataset', fontsize=12,title_fontsize=12, loc='lower left',
                        framealpha=1)
    for handle, label, hatch in zip(legend.legend_handles, legend.get_texts(), hatches):
        # handle.set_facecolor('white')
        handle.set_edgecolor('black')
        # handle.set_hatch(hatch+hatch+hatch+hatch)

    ################################
    # Add FK/Non-FK category bar at the top
    ax2 = ax.twiny()
    ax2.set_xlim(ax.get_xlim())

    # Create category labels and positions
    n_algs = len(algs)
    non_fk_end = 8.5  # Position between 9th and 10th algorithm (0-indexed: 8.5)

    # # Add category spans
    # ax2.axvspan(-0.5, non_fk_end, facecolor='lightblue', alpha=0.3,
    #             transform=ax2.transData, clip_on=False)
    # ax2.axvspan(non_fk_end, n_algs-0.5, facecolor='lightyellow', alpha=0.3,
    #             transform=ax2.transData, clip_on=False)
    #
    # # Add category labels
    # ax2.text(0.25, 1.02, 'Non-FK', ha='center', va='bottom',
    #          transform=ax2.transAxes, fontsize=12, fontweight='bold')
    # ax2.text(0.75, 1.02, 'FK', ha='center', va='bottom',
    #          transform=ax2.transAxes, fontsize=12, fontweight='bold')

    # Calculate label positions and widths
    non_fk_center = non_fk_end/2
    fk_center = (non_fk_end + n_algs-0.5)/2

    # Add category labels with background boxes
    bbox_props = dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.7, edgecolor='white', linewidth=1)
    ax2.text(0.25, 1.02, 'Non-FK joins', ha='center', va='bottom',
             transform=ax2.transAxes, fontsize=14, bbox=bbox_props)

    bbox_props = dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.7, edgecolor='white', linewidth=1)
    ax2.text(0.75, 1.02, 'FK joins', ha='center', va='bottom',
             transform=ax2.transAxes, fontsize=14, bbox=bbox_props)

    # Add rectangular boxes using XY coordinates
    from matplotlib.patches import Rectangle

    # Non-FK box (in data coordinates for x, axes coordinates for y)
    non_fk_box = Rectangle((1, 1.01), 3, 5,
                           facecolor='lightblue', alpha=0.7, edgecolor='black', linewidth=1,
                           transform=ax2.get_xaxis_transform())
    ax2.add_patch(non_fk_box)



    # Add dividing line between categories
    y_cord = 1.08
    ax2.axvline(x=-0.5, linestyle='-', color='black', linewidth=1,
                ymin=1.0, ymax=y_cord, clip_on=False)
    ax2.axvline(x=non_fk_end, linestyle='-', color='black', linewidth=1,
                ymin=1.0, ymax=y_cord, clip_on=False)
    ax2.axvline(x=15.5, linestyle='-', color='black', linewidth=1,
                ymin=1.0, ymax=y_cord, clip_on=False)
    # ax2.axhline(y=y_cord, linestyle='-', color='black', linewidth=1,
    #             xmin=-0.5, xmax=15.5, clip_on=False)

    # Hide the top axis ticks and labels
    ax2.set_xticks([])
    ax2.set_xticklabels([])

    # Adjust layout to accommodate the category bar
    plt.subplots_adjust(top=0.85)
    ################################

    savefig(out_name,tight_layout=True)


def plot_top_figure(algs, out_name):
    plt.rcParams.update(plt.rcParamsDefault)
    data = pd.read_csv(res_file)
    plt.figure(figsize=(5,2.5))
    data = data[(data['algorithm'].isin(algs)) & (data['dataset'] == 'synth-1')]
    algorithms = data['algorithm'].unique()

    hatches = ['o', '//']
    small_hatches = ['oo', '///']

    # plt.bar(data['algorithm'], data['inputTuples']/data['time'], tick_label=algorithms)

    throughputs = 1000*data['inputTuples']/data['time']
    data.insert(4, 'throughput', throughputs, True)
    # data.set_index('algorithm').plot(kind='bar',y='throughput')

    ax = sns.barplot(x='algorithm', y='throughput', data=data, order=algs)

    palette = [color_categorical(x) for x in algs]
    for bars, hatch in zip(ax.containers, hatches):
        for bar, color in zip(bars, palette):
            bar.set_facecolor(color)
            bar.set_edgecolor('black')
            # bar.set_hatch(hatch)
        # update the existing legend, use twice the hatching pattern to make it denser
        # legend_handle.set_hatch(hatch + hatch)
    # sns.despine()


    ax.set(xticklabels=['SHJ', '$\mathcal{L}_0$', '$\mathcal{L}_1$', '$\mathcal{L}_2$',
                        '$\mathcal{L}_3$', '$\mathcal{L}_4$', 'NLJ'])
    # colors = ['red', 'black', 'black', 'black', 'black', 'black', 'red']
    # labels = ['SHJ', '$\mathcal{L}_0$', '$\mathcal{L}_1$', '$\mathcal{L}_2$',
    #  '$\mathcal{L}_3$', '$\mathcal{L}_4$', 'NLJ']
    # ax.set_xticklabels(labels, color=colors)
    labels = ax.get_xticklabels()
    # labels[0].set_color('red')
    # labels[6].set_color('red')

    plt.xlabel('Level of data privacy')
    plt.ylabel('Throughput\n[K rec / s]')

    plt.yscale('log')
    # plt.minorticks_on()

    # plt.ylim(bottom=150)
    ax.tick_params(axis='x', which='major', labelsize=15)
    y = -0.6
    ax.annotate('', xy=(0.1, y), xycoords='axes fraction', xytext=(0.9, y),
                arrowprops=dict(arrowstyle="<-", color='black', linestyle="--"))
    # plt.xticks(rotation=30)
    # legend = plt.legend(title='dataset', fontsize='x-small')
    # for handle, label, hatch in zip(legend.legend_handles, legend.get_texts(), small_hatches):
    #     handle.set_facecolor('white')
    #     handle.set_edgecolor('black')
    #     handle.set_hatch(hatch)
    # commons.draw_horizontal_lines(plt, [3600*12])
    # plt.text(1000, 3600*13,'12h')
    plt.tight_layout()
    ax.yaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(axis='y', which='minor', bottom=False)

    savefig(out_name, tight_layout=False)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # algorithms = ['SHJ', 'NLJ-L4', 'FK-EPHI-L2', 'FK-MERG-L3', 'FK-MERG-L4', 'FK-SORT-L2', 'FK-SORT-L3', 'FK-SORT-L4', 'FK-MERG-L2',
    #               'SHJ-L0', 'SHJ-L1', 'SHJ-L2', 'SHJ-L3', 'SHJ-L4','NFK-JOIN-L2','NFK-JOIN-L3']
    algorithms = ['NFK-JOIN-L2','NFK-JOIN-L3']
    cfgs = [Config(name='synth-1',
                    algorithm = None,
                    r_rate=1024,
                    s_rate=1024,
                    r_size=200000,
                    s_size=200000,
                    r_window=65536,
                    s_window=65536,
                    r_batch=1024,
                    s_batch=1024,
                    skew=0,
                    fk_join=True,
                    nthreads=1),
            Config(name='synth-2',
                   algorithm = None,
                   r_rate=1024,
                   s_rate=4096,
                   r_size=200000,
                   s_size=800000,
                   r_window=65536,
                   s_window=65536,
                   r_batch=1024,
                   s_batch=4096,
                   skew=0,
                   fk_join=True,
                   nthreads=1),
            Config(name='tpch-1',
                   dataset='tpch-1',
                   algorithm=None,
                   r_rate=1024,
                   s_rate=1024,
                   r_window=65536,
                   s_window=65536,
                   r_batch=1024,
                   s_batch=4096,
                   nthreads=1)
            ]

    if config['experiment']:
        remove_file(res_file)
        init_file(res_file, "algorithm,dataset,inputTuples,time\n")
        if config['compile']:
            compile_app()
        for algorithm in algorithms:
            for cfg in cfgs:
                cfg.algorithm = algorithm
                join(cfg, config['repetitions'])

    if config['plot']:
        # plot(['SHJ', 'FK-EPHI-L2', 'SHJ-L2'], 'results/l2-general-performance.png')
        # plot(['SHJ', 'FK-MERG-L3', 'SHJ-L3'], 'results/l3-general-performance.png')
        # plot(['NLJ-L4', 'FK-MERG-L4', 'SHJ-L4'], 'results/l4-general-performance.png')
        plot(['SHJ','SHJ-L0','SHJ-L1', 'SHJ-L2', 'NFK-JOIN-L2', 'SHJ-L3', 'NFK-JOIN-L3', 'SHJ-L4', 'NLJ-L4',
              'FK-EPHI-L2', 'FK-MERG-L2', 'FK-SORT-L2', 'FK-MERG-L3', 'FK-SORT-L3', 'FK-MERG-L4', 'FK-SORT-L4', ],
             'results/all-general-performance.png')
        # top figure
        # plot_top_figure(['SHJ','SHJ-L0','SHJ-L1','FK-EPHI-L2','FK-MERG-L3','FK-MERG-L4', 'NLJ-L4'], 'results/top-figure.png')