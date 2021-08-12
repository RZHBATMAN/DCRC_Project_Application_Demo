# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 11:13:13 2018

@author: home
"""


import numpy as np
import pandas as pd

def build(db, dates, univ_or_insts = None, grace = 720, fac_construction_id = None, **kargs):

    items = [ 'FS_DTRC', 'FS_DVO' ]
    startdate, enddate = dates[0] - pd.offsets.Day(grace), dates[-1]
    dfs = db.load_raw(items, startdate, enddate, univ_or_insts)
    
    if fac_construction_id:
        db.save_raw_snapshots(dfs, fac_construction_id)
    
    ri = dfs['FS_DTRC']
    vo = dfs['FS_DVO']

    inst_ids = ri.columns
    df = db.load_attributes(inst_ids,['name','exchange'])
    exchange_dict = df.groupby('exchange').apply(lambda x:x['name'].tolist()).to_dict()

    exchs = list(exchange_dict.keys())
    exch_calend = {}
    for exch in exchs:
        exch_calend[exch] = db.load_calend(exch, startdate, enddate)
        

    for exch in exchs:

        td = exch_calend[exch]  
        instruments_for_this_exch = exchange_dict[exch]
        non_td = [i for i in dates if i not in td]
        ri.loc[list(set(non_td).intersection(set(ri.index))),instruments_for_this_exch] = np.nan
        vo.loc[list(set(non_td).intersection(set(vo.index))),instruments_for_this_exch] = np.nan
    
    #ri[np.logical_or(vo==0, np.isnan(vo))] = np.nan
    #exclude zero volume dates?    
    
    n = dates.size

    columns = ri.columns
    r_mov = None
    
    for x in range(n):
                
        temp = ri.loc[ri.index < dates[x]]
        temp = temp.loc[temp.index > (dates[x] - pd.offsets.DateOffset(years=1))] #TODO: 
        
        #then = time.time()
        temp = temp.values
        idx = (np.isnan(temp)).sum(axis=0)
        vol_temp = np.nanstd(temp, axis=0, ddof = 1) * 252**0.5
        vol_temp[idx>200] = np.nan
                
         
        if x==0:
            r_mov = vol_temp
        else:
            r_mov = np.vstack((r_mov, vol_temp))
                        
    r = pd.DataFrame(r_mov, dates, columns)

    return r