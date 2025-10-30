import csv
import errno
import getopt
import os
import re
import subprocess
import sys
from timeit import default_timer as timer
from datetime import timedelta
import matplotlib.pyplot as plt
from collections import namedtuple

import numpy as np
import pandas as pd
import yaml

PROG = ' ./app '

def start_timer():
    return timer()


def stop_timer(start):
    print("Execution time: " + str(timedelta(seconds=(timer()-start))))


def escape_ansi(line):
    ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
    return ansi_escape.sub('', line)


def compile_app(flags=None, config=None, debug=False, additional_vars=[]):
    if flags is None:
        flags = []
    print("Make clean")
    subprocess.check_output(["make", "clean"], cwd="../")

    cflags = 'CFLAGS='

    for flag in flags:
        cflags += ' -D' + flag + ' '

    if config is None:
        enclave_string = ' CONFIG=Enclave/Enclave.config.xml '
    else:
        enclave_string = ' CONFIG=' + str(config) + ' '

    print("Make enclave " + enclave_string + " with flags " + cflags)
    if debug:
        args = ["make", 'SGX_DEBUG=1', 'SGX_PRERELEASE=0',
                enclave_string, cflags] + additional_vars
        subprocess.check_output(args, cwd="../", stderr=subprocess.DEVNULL)
    else:
        args = ["make", 'SGX_DEBUG=0', enclave_string, cflags] + additional_vars
        subprocess.check_output(args, cwd="../", stderr=subprocess.DEVNULL)


def remove_file(fname):
    try:
        os.remove(fname)
    except FileNotFoundError:
        pass
    except OSError as e:
        raise

def init_file(fname, first_line):
    print(first_line)
    with open(fname, "w") as file:
        file.write(first_line)

##############
# colors:
#
# 30s
# mint: bcd9be
# yellow brock road: e5cf3c
# poppy field: b33122
# powder blue: 9fb0c7
# egyptian blue: 455d95
# jade: 51725b
#
# 20s
# tuscan sun: e3b22b
# antique gold: a39244
# champagne: decd9d
# cadmium red: ad2726
# ultramarine: 393c99
# deco silver: a5a99f
#
# 40s
# war bond blue: 30356b
# keep calm & canary on: e9ce33
# rosie's nail polish: b90d08
# air force blue: 6d9eba
# nomandy sand: d1bc90
# cadet khaki: a57f6a
#
# 80s
# acid wash: 3b6bc6
# purple rain: c77dd4
# miami: fd69a5
# pacman: f9e840
# tron turqouise: 28e7e1
# powersuit: fd2455
#
# 2010s
# millennial pink: eeb5be
# polished copper: c66d51
# quartz: dcdcdc
# one million likes: fc3e70
# succulent: 39d36e
# social bubble: 0084f9
#############


def color_categorical(i):
    colors = {
        "SHJ-L0":'#0fb5ae',
        "SHJ-L1":'#4046ca',
        "SHJ-L2":'#f68511',
        "SHJ-L3":'#de3d82',
        "SHJ-L4":'#7e84fa',
        "FK-EPHI-L2":'#72e06a',
        "FK-MERG-L3":'#147af3',
        "FK-MERG-L4":'#7326d3',
        "SHJ":'#e8c600',
        'FK-MERG-L2':'#cb5d00',
        'OPAQUE':'#008f5d',
        "NLJ-L4":'#bce931',

        'FK-SORT-L2':'#b44b20',  # burnt sienna
        'FK-SORT-L3':'#7b8b3d',  # avocado
        'FK-SORT-L4':'#c7b186',  # natural
        'NFK-JOIN-L3':'#885a20',  # teak
        'NFK-JOIN-L2':'#e6ab48',  # harvest gold
        'FK-MERG-L4-SPLIT':'#4c748a',  # blue mustang
    }
    return colors[i]


def color_alg(alg):
    # colors = {"CHT":"#e068a4", "PHT":"#829e58", "RHO":"#5ba0d0"}
    colors = {"CHT":"#b44b20",  # burnt sienna
              "PHT":"#7b8b3d",  # avocado
              "PSM":"#c7b186",  # natural
              "BHJ":"#c7b186",
              "RHT":"#885a20",  # teak
              "RHO":"#e6ab48",  # harvest gold
              "RSM":"#4c748a",  # blue mustang
              'TinyDB': '#0084f9',  # avocado
              "OJ":"#fd2455",
              "OPAQ":"#c77dd4",
              "OBLI":"#3b6bc6",
              "INL":'#7620b4',
              'NL':'#20b438',
              'MWAY':'#fd2455',
              # 'PaMeCo':'#28e7e1',
              'PaMeCo':'#bcd9be',
              'CRKJ':"#b3346c", # moody mauve
              "CRKJS":"#deeppink",
              'CrkJ':"#b3346c", # moody mauve
              'CrkJoin':"#b3346c", # moody mauve
              'CrkJoin+Skew':"#7620b4", # moody mauve
              'GHJ':'black',
              'RHO_atomic':'deeppink',
              # 'MCJoin':'#0084f9', # social bubble
              'MCJoin':'#51725b', # social bubble
              'CRKJ_CHT':'deeppink',
              'oldCRKJ':"#7b8b3d", # avocado
              'CRKJ_static':"#4c748a", # blue mustang

              'RHO-sgx': '#e6ab48',
              'RHO-sgx-affinity':'g',
              'RHO-lockless':'deeppink',
              'RHO_atomic-sgx':'deeppink'}
    # colors = {"CHT":"g", "PHT":"deeppink", "RHO":"dodgerblue"}
    return colors[alg]


def marker_alg(alg):
    markers = {
        "CHT": 'o',
        "PHT": 'v',
        "PSM": 'P',
        "RHT": 's',
        "RHO": 'X',
        "RSM": 'D',
        "INL": '^',
        'MWAY': '*',
        'RHO_atomic': 'P',
        'CRKJ':'>',
        'CrkJoin':'>',
        'CrkJoin+Skew':'D',
        'CRKJoin':'>',
        'CRKJS':'*',
        'CRKJ_CHT':'^',
        'oldCRKJ':'X',
        'CRKJ_static': 'D',
        "MCJoin": 'o',
        'PaMeCo': 'P',
    }
    return markers[alg]


def savefig(filename, font_size=15, tight_layout=True):
    plt.rcParams.update({'font.size': font_size})
    if tight_layout:
        plt.tight_layout()
    plt.savefig(filename, transparent = False, bbox_inches = 'tight', pad_inches = 0.1, dpi=200)
    print("Saved image file: " + filename)


def draw_vertical_lines(plt, x_values, linestyle='--', color='#209bb4', linewidth=1):
    for x in x_values:
        plt.axvline(x=x, linestyle=linestyle, color=color, linewidth=linewidth)


def draw_horizontal_lines(plt, y_values, linestyle='--', color='#209bb4', linewidth=1):
    for y in y_values:
        plt.axhline(y=y, linestyle=linestyle, color=color, linewidth=linewidth)


def draw_horizontal_line(plt, y_value, linestyle='--', color='#209bb4', linewidth=1):
    plt.axhline(y=y_value, linestyle=linestyle, color=color, linewidth=linewidth)


def read_config(input_argv, yaml_file='config.yaml'):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    argv = input_argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'r:',['reps='])
    except getopt.GetoptError:
        print('Unknown argument')
        sys.exit(1)
    for opt, arg in opts:
        if opt in ('-r', '--reps'):
            config['reps'] = int(arg)
    return config


def flatten(xss):
    return [int(x) for xs in xss for x in xs]


def get_config_1(algorithm: str = None):
    return Config(name='synth-1',
                  algorithm = algorithm,
                  r_rate=1000,
                  s_rate=1000,
                  r_size=2000000,
                  s_size=2000000,
                  r_window=1000,
                  s_window=1000,
                  batch=500,
                  skew=0,
                  fk_join=True)


def get_config_2(algorithm: str = None):
    return Config(name='synth-2',
                  algorithm = algorithm,
                  r_rate=1000,
                  s_rate=1000,
                  r_size=2000000,
                  s_size=2000000,
                  r_window=1000,
                  s_window=1000,
                  batch=500,
                  skew=0.9,
                  fk_join=True)


def next_power_of_two(n):
    # If n is already a power of 2, return it
    if n & (n - 1) == 0:
        return n
    # Otherwise, calculate the next power of 2
    return 1 << (n.bit_length())


def evenly_log_distributed_points(start_value, end_value, num_points):
    # Calculate the logarithm (base 10) of start and end values
    start_exponent = np.log10(start_value)
    end_exponent = np.log10(end_value)

    # Generate log scale points between start and end values
    log_scale_points = np.logspace(start_exponent, end_exponent, num=num_points)

    # Convert points to integers
    ints = np.round(log_scale_points).astype(int)
    final = [next_power_of_two(int(x)) for x in ints]
    return final


class Config:
    def __init__(self, algorithm: str = None, r_batch: int = None, s_batch: int = None, r_rate: int = None,
                 s_rate: int = None, r_size: int = None, s_size: int = None, r_window: int = None,
                 s_window: int = None, skew: float = None, fk_join: bool = None, self_join: bool = None,
                 no_sgx: bool = None, nthreads: int = None, name: str = None, dataset: str = None):
        self.s_window = s_window
        self.r_window = r_window
        self.s_size = s_size
        self.r_size = r_size
        self.s_rate = s_rate
        self.r_rate = r_rate
        self.r_batch = r_batch
        self.s_batch = s_batch
        self.skew = skew
        self.algorithm = algorithm
        self.self_join = self_join
        self.fk_join = fk_join
        self.no_sgx = no_sgx
        self.nthreads = nthreads
        self.name = name
        self.dataset = dataset
        if self.algorithm == 'SHJ' or self.algorithm == 'SHJ-L0':
            self.no_sgx = True

    def __str__(self):
        return ("Config(name: " + str(self.name) + ", algorithm: " + str(self.algorithm) +
                "\n\tR Stream: size=" + str(self.r_size) + " rate=" + str(self.r_rate) +
                " window=" + str(self.r_window) + " batch=" + str(self.r_batch) +
                "\n\tS Stream: size=" + str(self.s_size) + " rate=" + str(self.s_rate) +
                " window=" + str(self.s_window) + " batch=" + str(self.s_batch) + " skew=" + str(self.skew) +
                "\n\t" + " FK-join=" + str(self.fk_join) +
                " self-join=" + str(self.self_join) + " no-sgx=" + str(self.no_sgx) +
                " nthreads=" + str(self.nthreads) + " dataset=" + str(self.dataset) +
                ")")

    def command(self):
        command = './app '
        command += ' --alg ' + str(self.algorithm) + ' ' if self.algorithm else ''
        command += ' --dataset ' + str(self.dataset) if self.dataset else ''
        command += ' --r-batch ' + str(self.r_batch) + ' ' if self.r_batch else ''
        command += ' --s-batch ' + str(self.s_batch) + ' ' if self.s_batch else ''
        command += ' --r-rate ' + str(self.r_rate) + ' ' if self.r_rate else ''
        command += ' --s-rate ' + str(self.s_rate) + ' ' if self.s_rate else ''
        command += ' --r-size ' + str(self.r_size) + ' ' if self.r_size else ''
        command += ' --s-size ' + str(self.s_size) + ' ' if self.s_size else ''
        command += ' --r-window ' + str(self.r_window) + ' ' if self.r_window else ''
        command += ' --s-window ' + str(self.s_window) + ' ' if self.s_window else ''
        command += ' --skew ' + str(self.skew) + ' ' if self.skew else ''
        command += ' --fk-join ' if self.fk_join else ''
        command += ' --self-join ' if self.self_join else ''
        command += ' --no-sgx ' if (self.no_sgx or self.algorithm=='SHJ' or self.algorithm=='SHJ-L0') else ''
        command += ' --nthreads ' + str(self.nthreads) if self.nthreads else ''
        return command
