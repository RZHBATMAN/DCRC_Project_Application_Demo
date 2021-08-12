# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""

import numpy as np
import pandas as pd


def build(db, dates, univ_or_insts = None,  grace = 720, fac_construction_id = None, **kargs):

    items = [ 'FS_PHUNADJ', 'FS_SPLITFAC', 'FS_PUNADJ' ]
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)

    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    ph = dfs['FS_PHUNADJ']
    adj = dfs['FS_SPLITFAC']
    p = dfs['FS_PUNADJ']
    
    p = p.reindex(index = adj.index, columns = adj.columns)
    ph = ph.reindex(index = adj.index, columns = adj.columns)

    adj = adj.shift(-1)
    adj.iloc[-1,:] = 1
    adj[adj==0] = 1
    temp = np.cumprod(adj[::-1], axis=0)
    adj = temp[::-1]
    
    ph = ph*adj
    p = p*adj
    
    n = dates.size

    for x in range(n):
                
        temp = ph.loc[ph.index <= dates[x]]
        temp = temp.loc[temp.index > (dates[x] - pd.offsets.DateOffset(years=1))]
        
        p_temp = p.loc[p.index <= dates[x]]
        p_temp = p_temp.loc[p_temp.index > (dates[x] - pd.offsets.DateOffset(years=1))]
        
        col = temp.columns
        
        ph_temp = temp.max(axis=0) 
        p_temp = p_temp.tail(1)
        ph_check = temp.tail(1)
        
        #then = time.time()
        ph_temp = ph_temp.values
        p_temp = p_temp.values
        
        php = p_temp/ph_temp
        php[np.isnan(ph_check)] = np.nan        
         
        if x==0:
            r_mov = php
        else:
            r_mov = np.vstack((r_mov, php))
            
#        now = time.time()
#        print("It took: ", (now-then), " seconds")
            
    r = pd.DataFrame(r_mov, dates, col)  # TODO: remove unbound warning
    
    return r
