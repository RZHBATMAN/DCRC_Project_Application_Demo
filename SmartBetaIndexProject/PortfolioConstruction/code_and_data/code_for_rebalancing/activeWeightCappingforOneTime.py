# -*- coding: utf-8 -*-
"""
Created on Fri Aug 28 09:46:47 2020

@author: Zhihao(Chris)Ren
"""

import pdb
import pandas as pd
import numpy as np


def active_weight_capping_for_one_time(original_market_cap_weight,w1,min_active_cap,max_active_cap):
    
    
    
    w1 = w1.reindex(original_market_cap_weight.index).fillna(0)
    
    fixed_stock_list = []
    
    while(True):

        act_weight = w1['weight'].values-original_market_cap_weight['marketcap_weight'].values
    
        if (np.sum(act_weight<min_active_cap-10**(-5))==0) and (np.sum(act_weight>max_active_cap+10**(-5))==0):
            break

        new_added_fixed = w1.index[act_weight<min_active_cap-10**(-5)].tolist()+\
                          w1.index[act_weight>max_active_cap+10**(-5)].tolist()
        fixed_stock_list+= new_added_fixed
        
        act_weight[act_weight<min_active_cap-10**(-5)] = min_active_cap
        act_weight[act_weight>max_active_cap+10**(-5)] = max_active_cap

        w1['weight']=act_weight+original_market_cap_weight['marketcap_weight'].values
    
        changable_list = [i for i in w1.index if i not in fixed_stock_list]

    
        w1.loc[changable_list,'weight']=(1-w1.loc[fixed_stock_list,'weight'].sum())*\
            w1.loc[changable_list,'weight']/w1.loc[changable_list,'weight'].sum()
        
        if(w1['weight'].sum()!=1):
            print(f"Stock weight sum is {w1['weight'].sum()}, something goes wrong!")
 
    
    w1 = w1[w1['weight']!=0]
    
    
    return w1 
    
    