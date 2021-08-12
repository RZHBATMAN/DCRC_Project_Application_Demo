# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 18:23:03 2018
@author: home
"""

import importlib
import time
import pickle
import json
import sys, os
import pdb

import pandas as pd

from db import DB

# We can use this function to iterate over the factorlib folder get a list of all factors
def get_factor_list(faclib_dir):
    """ Get a factor list from the folder where the factor python definition files exist """

# fac_list = pandas.read_excel(f'{db.work_path}FactSet_Control.xlsm', sheet_name='FactorList')['FactorList']

    import glob    # for iterating a folder
    import os      # for extracting file name


    fac_list = []
    for filepath in glob.iglob(f'{faclib_dir}/*.py'):
        f = os.path.basename(os.path.splitext(filepath)[0])
        if f:
            fac_list.append(f)
    
    return fac_list


"""
    Factor Build Parameters Explanation:

    db: DB instance, should be opened, used for access database
    freq:      pandas offset object or frequency string. An example of offset object is:
                pd.offsets.WeekOfMonth(n=1, week=options['week_of_month'], weekday=options['day_of_week']) 
                which represents 6 month spaced, 3rd week, friday
    industry:  industry name, e.g., HSICS, GICS, ICB etc.
    univ:      universe name. 
    inst_list: instrument list.

    Note that only one of univ and inst_list should be provided; if both given, univ trumps.
    If none of them given, then all instrument in the specified period are assumed.

    Possible Improvements:
    1. Change startdate, enddate, and freq into a dates.
    2. Options in args for processing dates and filling.
"""

if __name__ == '__main__':
    
    #append all paths containing code
    base_path = os.path.dirname(sys.argv[0])

    def mk_abspath(p): return p if os.path.isabs(p) else os.path.join(base_path, p)

    config_file = 'config.json' if len(sys.argv) < 2 else sys.argv[1]
    config_file = mk_abspath(config_file)

    # DB constructor parameters. Pls change based on your setting.
    with open(config_file, 'r') as cf:
        info = cf.read()

    config = json.loads(info)

    index_list = config['index_list']
    fac_list   = config['fac_list']
    if type(fac_list) is str:
        fasc_list = get_factor_list(fac_list)  # treat fac_list a Faclib path

    rawdata_list = config['rawdata_list']

    dates = pd.date_range(start=config['startdate'], end=config['enddate'], freq=config['freq'], normalize=True)

    grace = config['grace']

    industry = 'HSICS'   # TODO: to be removed!

    dbcfg = config['db']
    db = DB(dbcfg["work_dir"], dbcfg["user"], dbcfg["password"])

    factors = {}

    saved_raw_list = []

    with db:
        fac_construct_id = db.gen_fac_construct_id(info)

       
        #pdb.set_trace()

        for index in index_list:
            print(f'For {index}:')

            for f in fac_list:
                then = time.time()
                print(f'    Computing {f} ... ', end='')        
                module = importlib.import_module(f'FactorLib.{f}')
                df = module.build(db, dates, index, grace = grace, fac_construction_id = fac_construct_id)
                dfs = {f:df}
                db.save_fac(dfs, fac_construct_id)
                now = time.time()
                print(f'done in {now-then:.2f} seconds.')


            #find all used raw data
            #rawdata_list = .... raw_data + industry
            #startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
            #dfs = db.load_raw(rawdata_list, startdate, enddate, index)
            #db.save_raw_snapshots(dfs, fac_construct_id) 
            pickle.dump(factors, open(f'{index}.pickle', 'wb'))  
