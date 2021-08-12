# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 10:30:06 2020

@author: Zhihao(Chris)Ren
"""

import sys
import os
import json
import pandas as pd
import numpy as np
from scipy.stats import norm

#append all paths containing code
base_path = os.path.dirname(sys.argv[0])

sys.path += [
    os.path.join(base_path, 'code_for_analysis'),
    os.path.join(base_path, 'code_for_rebalancing'),
    os.path.join(base_path, '../../FactorConstruct/src/')
]


def mk_abspath(p): return p if os.path.isabs(p) else os.path.join(base_path, p)

config_file = 'config_prd.json' if len(sys.argv) < 2 else sys.argv[1]
config_file = mk_abspath(config_file)
config = json.load(open(config_file, 'r'))

parameters1 = {**config['parameters'], **config['parameters_review']}
parameters2 = {**config['parameters'], **config['parameters_capping']}
hyper_parameters = config['hyper_parameters']

hyper_parameters['db']['work_directory'] = mk_abspath(hyper_parameters['db']['work_directory'])
for f in ['production_on_review_input_file', 
          'production_on_capping_input_file', 
          'production_on_review_output_file', 
          'production_on_capping_output_file']:
    hyper_parameters[f] = mk_abspath(hyper_parameters[f])




"""
officially run the function below
"""

from indexSelection import index_selection

#on review date
index_dict = index_selection(
                    hyper_parameters = hyper_parameters, 
                    parameters = parameters1,
                    stage = 'production_on_review', 
                    previous_index = []
                    )

#on capping date
w_final = index_selection(
                    hyper_parameters = hyper_parameters, 
                    parameters = parameters2,
                    stage = 'production_on_capping', 
                    previous_index = []
                    )
