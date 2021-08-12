# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import pandas as pd
from mapdates import getData

def build(db, dates, univ_or_insts = None, grace = 720, fac_construction_id = None, **kargs):

    items = [ 'FS_ROE', 'FS_EPS_RPT_DATE', 'FS_FISCAL_DATE' ]
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)

    ROE = dfs['FS_ROE']    
    ada = dfs['FS_EPS_RPT_DATE']
    fiscal = dfs['FS_FISCAL_DATE']
    
    ROE = getData(dates, fiscal, ada, ROE)

    return ROE    
    
