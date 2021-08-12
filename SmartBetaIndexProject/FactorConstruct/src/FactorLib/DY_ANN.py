# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import numpy as np
import pandas as pd
from mapdates import getData

def build(db, dates, univ_or_insts = None, grace = 720, fac_construction_id = None, **kargs):
    
    items = ['FS_EPS_RPT_DATE', 'FS_FISCAL_DATE', 'FS_DPS', 'FS_SPLITFAC', 'FS_PUNADJ']
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)
    
    ada = dfs['FS_EPS_RPT_DATE']    
    fiscal = dfs['FS_FISCAL_DATE']    
    DPS = dfs['FS_DPS']
    
    DPS = getData(dates, fiscal, ada, DPS)
    
    adj = dfs['FS_SPLITFAC']
    p = dfs['FS_PUNADJ']
    p = p.reindex(index = adj.index, columns = adj.columns)
    p = p.fillna(method='ffill')
        
    adj = adj.shift(-1)
    adj.iloc[-1,:] = 1
    adj[adj==0] = 1
    temp = np.cumprod(adj[::-1], axis=0)
    adj = temp[::-1]

    p = p*adj
    p = p.reindex(index = dates, method = 'ffill')
 
    DY = DPS/p    
    return DY 
    
