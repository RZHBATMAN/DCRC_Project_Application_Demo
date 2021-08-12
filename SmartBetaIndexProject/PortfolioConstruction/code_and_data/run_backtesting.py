# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 10:30:06 2020

@author: Zhihao(Chris)Ren
"""


import pandas as pd
import numpy as np
import sys, os
import json
import pickle
import math
from scipy.stats import norm
import pdb

#append all paths containing code
base_path = os.path.dirname(sys.argv[0])

sys.path += [
    os.path.join(base_path, 'code_for_analysis'),
    os.path.join(base_path, 'code_for_rebalancing'),
    os.path.join(base_path, '../../FactorConstruct/src/')
]


from indexSelection import index_selection



def mk_abspath(p): return p if os.path.isabs(p) else os.path.join(base_path, p)

config_file = 'config_backtesting.json' if len(sys.argv) < 2 else sys.argv[1]
config_file = mk_abspath(config_file)
config = json.load(open(config_file, 'r'))

parameters = config['parameters']
hyper_parameters = config['hyper_parameters']

hyper_parameters['db']['work_directory'] = mk_abspath(hyper_parameters['db']['work_directory'])
hyper_parameters['backtesting_output_file'] = mk_abspath(hyper_parameters['backtesting_output_file'])
    

rebalance_dates = config['rebalance_dates']
data_dates = config['data_dates']



"""

officially run the function below

"""

rebalance_df = index_selection(
                    rebalance_dates = rebalance_dates,
                    data_dates = data_dates, 
                    hyper_parameters = hyper_parameters, 
                    parameters = parameters, 
                    stage = 'back_testing', 
                    previous_index = [],
                    options = None
                    )