# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import numpy as np
import pandas as pd
from mapdates import getData

def build(db, dates, univ_or_insts = None, grace = 365*6, fac_construction_id = None, **args):
    
    items = ['FS_EPS', 'FS_EPS_RPT_DATE', 'FS_FISCAL_DATE']
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)
    
    for n in dfs:
        dfs[n] = dfs[n].resample('1Y').last()
    
    EPS = dfs['FS_EPS']
    ada = dfs['FS_EPS_RPT_DATE']
    fiscal = dfs['FS_FISCAL_DATE']
    
    df = (EPS - EPS.shift(1))/EPS.shift(1)
    df[EPS.shift(1)<0] = -df[EPS.shift(1)<0]
    df[EPS.shift(1)==0] = np.nan
    
    df[df>5] = 5
    df[df<-5] = -5 
    
    std_temp = df.rolling(4).std()
    
    vari = getData(dates, fiscal, ada, std_temp)  
    
    return vari    
    
