# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import numpy as np
import pandas as pd
from mapdates import getData

def build(db, dates, univ_or_insts = None, grace = 720, fac_construction_id = None, *args):

    items = ['FS_TOTAL_ASSET', 'FS_CSTI', 'FS_TOTAL_LIAB', 'FS_TOTAL_DBET', 'FS_EPS_RPT_DATE'
             , 'FS_FISCAL_DATE']
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    ind = db.load_raw('HS_IND', startdate, enddate, univ_or_insts)  
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)
        db.save_raw_snapshots(ind, fac_construction_id)
    
    industry = ind['HS_IND']
    industry = industry.reindex(index=dates, method = 'ffill')
    
    for n in dfs:
        dfs[n] = dfs[n].resample('1Y').last()
    
    
    dfs['FS_CSTI'].fillna(0)
    
    NOA = (dfs['FS_TOTAL_ASSET'] - dfs['FS_CSTI']) - (dfs['FS_TOTAL_LIAB'] - dfs['FS_TOTAL_DBET'])
    AR = (NOA - NOA.shift(1))/(NOA + NOA.shift(1))*2
               
    AR = getData(dates, dfs['FS_FISCAL_DATE'], dfs['FS_EPS_RPT_DATE'], AR)
    
    AR[industry == 50] = np.nan
    
    return AR    
    
