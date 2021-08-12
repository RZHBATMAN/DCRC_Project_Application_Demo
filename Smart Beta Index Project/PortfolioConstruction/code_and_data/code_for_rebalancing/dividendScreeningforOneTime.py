# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 10:52:30 2020

@author: Zhihao(Chris)Ren
"""

import pandas as pd
import numpy as np
import pdb
from mapdates import getData 
from mapdates import convert_excel_time

def dividend_screening_for_one_time(universe_list, data_date ,previous_index , DY_ANN, WS_ADA, WS_FISCAL, WS_PO) :
    
    #pdb.set_trace()
    #screening 1
    DY_ANN = DY_ANN.reindex(columns = universe_list,copy=True)
    DY_ANN.fillna(0,inplace=True)
    
    latest_time = DY_ANN.index.asof(data_date)
    annual_df = DY_ANN.loc[:latest_time].resample('A').apply(lambda x:x[-1])
    
    screened_list = []
    
    #insufficient data
    if(len(annual_df)<3):
        for stock in annual_df.columns:
            #no 0
            if sum(annual_df.loc[:,stock]==0)==0:
                screened_list.append(stock)
            else:
                continue

    
    #sufficient data
    else:
        for stock in annual_df.columns:
            check_df = annual_df.loc[:,stock].rolling(3)\
                .apply(lambda x:np.count_nonzero(x)==len(x))
            if check_df.sum()!=0:
                screened_list.append(stock)
            else:
                continue
    
    
    DY_ANN = DY_ANN.reindex(columns=screened_list)
    annual_df = annual_df.reindex(columns=screened_list)
    
    
    #screening 2
    WS_PO = getData([data_date], WS_FISCAL, WS_ADA, WS_PO).reindex(columns=screened_list)
    

    screened_list=[]
    for i,stock in enumerate(WS_PO.columns):
        if (WS_PO.iloc[0,i]>0) and (WS_PO.iloc[0,i]<100):
            screened_list.append(stock)

    #pdb.set_trace()


    
    #screening 3 
    fiscal_original = WS_FISCAL.reindex(columns=screened_list)
    ada = WS_ADA.reindex(columns=screened_list)
    
    
    fiscal = fiscal_original #.applymap(convert_excel_time)

    
    fiscal_difference = fiscal - fiscal.shift(1)
    #pdb.set_trace()
    fiscal_difference = fiscal_difference.applymap(lambda x:x.days if pd.isnull(x)==False else 0)
    fiscal_difference.fillna(0,inplace=True)

    

    fiscal_difference_final = getData([data_date], fiscal_original, ada, fiscal_difference) #data date
    fiscal_difference_final.fillna(0,inplace=True)
    changed_list = fiscal_difference_final.T[np.logical_and(fiscal_difference_final.T.values!=365,
                                             fiscal_difference_final.T.values!=366)].index.tolist()
    

    
    exclude_list1 = list(set(universe_list).difference(previous_index).intersection(changed_list))
    
    
    #screening 4 (remove too big divdend yield, namely, >0.07)
    DY_ANN = DY_ANN.reindex(columns=screened_list)
    df = DY_ANN.loc[latest_time,:].T
    exclude_list2 = df.index[df>0.07].tolist()
    
    
 
    #finalized
    screened_list = [i for i in screened_list if (i not in exclude_list1) and (i not in exclude_list2)]
    
    screened_index = sorted([universe_list.index(i) for i in screened_list])
    screened_list = [universe_list[i] for i in screened_index]
    

    
    return screened_list
    




