# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 20:00:25 2020

@author: Wellness2
"""


import pandas as pd
import numpy as np

def getData(dates, fiscal, ada, factor, threshold = 360*2):
    '''
      indexes of ada and factor are the date to financial data,
      columns are announcement dates of specific stocks.
      dates is dates (list-like) you are on for retriving data.
    '''

    t,n = ada.shape
    res = np.empty((len(dates),n))
    res[:] = np.nan
    ada = ada.fillna(method='bfill')
    fiscal = fiscal.fillna(method='bfill')

    dates = pd.DatetimeIndex(dates)

    mask = np.empty(t, dtype=bool)
    mask[:] = True
    
    #pdb.set_trace()
    for i in range(n): # over each stock
        # datemap = pd.Series(ada.index, index=ada.iloc[:,i]) # ada.index should be type of DatatimeIndex
        # try:
        #     index = datemap.asof(dates)
        #     res[:, i] = factor.loc[index, ada.columns[i]]
        # except ValueError as e:
        #     print(ada.columns[i], e)
        #     pass
        #     # in case of except (e.g., all dates are NaTs, res would be nans)
        
        dd = convert_pd_time(fiscal.iloc[:,i].values)
        locs = dd.asof_locs(dates, mask)
        
        locs_i = locs;
        while True:
            idx = np.where(locs_i >= 0, locs_i, 0)
            cond = (convert_pd_time(ada.iloc[idx,i]) > dates).values
            if np.any(cond[locs_i >= 0]):
                locs_i = np.where(locs_i >= 0, locs_i - cond, locs_i)
            else:
                break;
        
        #res[:,i] = factor.loc[ada.index[locs_i], ada.columns[i]]
        idx = np.where(locs_i<0, 0, locs_i)  # cached for time-saving purpose
        res[:,i] = factor.iloc[idx, i]  # assuming data aligned
        res[locs_i<0,i] = np.nan
        res[dates - pd.to_datetime(factor.index[idx]) > pd.Timedelta(threshold, 'D'),i] = np.nan

    return pd.DataFrame(res, index=dates, columns=ada.columns)

def convert_excel_time(excel_time):
    return pd.to_datetime('1899-12-30') + pd.to_timedelta(excel_time,'D')

def convert_pd_time(pytime):  # convert a python datatime.date to pd.DatatimeIndex
    return pd.to_datetime(pytime)

# Read excel sheets with proper but specific parameter; may need to change accordingly against content of sheets    
# data = pd.read_excel('/Users/pliu/neo/RiverMap/FactorAnalyzer/data_store/D_FactSet_Data_FA.xlsx', sheet_name=['ADA', 'ROE'], header=2, index_col=0)
# ada = data['ADA']
# roe = data['ROE']
# roe.index = pd.to_datetime(roe.index.map(convert_excel_time))  # this to avoid manually edit excel sheets

# a = getData(['2005-01-20', '2009-05-28', '2015-10-10'], ada, roe)
# print(a)