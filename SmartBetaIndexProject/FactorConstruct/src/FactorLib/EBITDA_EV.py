# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import numpy as np
import pandas as pd
from mapdates import getData

def build(db, dates, univ_or_insts = None, fac_construction_id = None, grace = 720, **args):
    
    items = ['FS_EPS_RPT_DATE', 'FS_FISCAL_DATE', 'FS_EBITDA', 'FS_EV']
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    ind = db.load_raw('HS_IND', startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)
        db.save_raw_snapshots(ind, fac_construction_id)
    
    for n in dfs:
        dfs[n] = dfs[n].resample('1Y').last()
    
    ada = dfs['FS_EPS_RPT_DATE']
    fiscal = dfs['FS_FISCAL_DATE']    
    R1 = dfs['FS_EBITDA']    
    R2 = dfs['FS_EV']
    
    R1 = getData(dates, fiscal, ada, R1)
    R2 = getData(dates, fiscal, ada, R2)
    
    R = R1/R2
    
    industry = ind['HS_IND']
    industry = industry.reindex(index=dates, method = 'ffill')
    
    R[industry == 50] = np.nan
    
    return R 
    
