# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import numpy as np
import pandas as pd
from mapdates import getData

def build(db, dates, univ_or_insts = None, grace = 720, fac_construction_id = None, **kargs):
    
    items = [
        'IBES_FY1_MEDIAN_EPS', 
        'IBES_FY2_MEDIAN_EPS', 
        'IBES_FY3_MEDIAN_EPS', 
        'IBES_FISCAL_FY1',
        'IBES_FISCAL_FY2',
        'FS_EPS_RPT_DATE',
        'FS_FISCAL_DATE',
        'FS_EPS',
        'FS_SPLITFAC',
        'FS_PUNADJ',
        
    ]
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)

    def align_time(df): return df.reindex(index=dates, method='ffill')  # python style recommend this instead of lambda
    items = [
        'IBES_FY1_MEDIAN_EPS', 
        'IBES_FY2_MEDIAN_EPS', 
        'IBES_FY3_MEDIAN_EPS', 
        'IBES_FISCAL_FY1',
        'IBES_FISCAL_FY2',
    ]

    for f in items:  
         dfs[f] = align_time(dfs[f])   # align in-place
    
    eps1 = dfs['IBES_FY1_MEDIAN_EPS']
    eps2 = dfs['IBES_FY2_MEDIAN_EPS']
    eps3 = dfs['IBES_FY3_MEDIAN_EPS']
    
    fy1 = dfs['IBES_FISCAL_FY1']
    fy2 = dfs['IBES_FISCAL_FY2']
    
    m1 = dateMonDiff(fy1)
    m2 = dateMonDiff(fy2)
    
    idx = m1>=0
    idx2 = m1<0
    
    fwd12 = eps1.copy()
    fwd12[:] = np.nan
    
    fwd12[idx] = eps1[idx].values*m1[idx].values/12 + eps2[idx].values*(12 - m1[idx].values)/12
    fwd12[idx2] = eps2[idx2].values*m2[idx2].values/12 + eps3[idx2].values*(12 - m2[idx2].values)/12
    
    ada = dfs['FS_EPS_RPT_DATE']
    fiscal = dfs['FS_FISCAL_DATE']
    r1 = dfs['FS_EPS']
    
    r1 = getData(dates, fiscal, ada, r1)

    fwd12[np.isnan(fwd12)] = r1[np.isnan(fwd12)]
    
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
     
    ep12 = fwd12/p       
    return ep12
 
def dateMonDiff(datefts):
    
    import Monthly_Run_test as md
    
    dy = 12*(md.convert_excel_time_matrix(datefts, 'year')\
            - np.tile(datefts.index.year.values, [datefts.shape[1],1]).T)
        
    dm =  (md.convert_excel_time_matrix(datefts, 'month')\
            - np.tile(datefts.index.month.values, [datefts.shape[1],1]).T) 
    
    d = dy + dm

    d = pd.DataFrame(d, index = datefts.index, columns = datefts.columns)  
        
    return d       
