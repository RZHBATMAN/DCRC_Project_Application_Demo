# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 10:40:20 2020

@author: Zhihao(Chris)Ren
"""

import numpy as np
import pandas as pd
import pdb

def final_capping_for_one_time(universe_list,original_market_cap_weight,
                         w2,max_cap_ratio,max_stock_weight,
                         is_active_weight_capping,
                         min_active_cap,max_active_cap):


    this_market_weight_df = original_market_cap_weight.reindex(index=w2.index)
    this_market_weight_df['capacity'] = max_cap_ratio*this_market_weight_df['marketcap_weight']
    
    info_df = pd.merge(w2,this_market_weight_df[['capacity']],left_index=True,right_index=True,how='left')
    info_df['max_weight'] = np.repeat(max_stock_weight,info_df.shape[0])


    if(is_active_weight_capping):
        info_df['min_according_to_active_weight'] = this_market_weight_df['marketcap_weight'].values - min_active_cap
        info_df['max_according_to_active_weight'] = this_market_weight_df['marketcap_weight'].values + max_active_cap
    

    i = 0
    
    while(i<40):

        if(is_active_weight_capping):
            temp_weight = info_df[['weight','capacity','max_weight','max_according_to_active_weight']].min(axis=1).values
            temp_weight = np.vstack([info_df['min_according_to_active_weight'].values,temp_weight]).max(axis=0)
            temp_weight = temp_weight / np.sum(temp_weight)
        
        else:
            temp_weight = info_df.min(axis=1).values
            temp_weight = temp_weight / np.sum(temp_weight)

        
        if sum(np.isclose(info_df['weight'].values,temp_weight))==len(temp_weight): #almost close
            break 
        
        info_df['weight'] = temp_weight
        
        i+=1
   
    #pdb.set_trace()   
    
    if(i==40):
        print('OMG! We have reached the maximum iteration number for capping {}!'.format(i))
    
    w3 = info_df[['weight']]
    w3 = w3.reindex(index=universe_list)
    w3.fillna(value=0,inplace=True)
    w3['weight'] = w3['weight'] / w3['weight'].sum()
    
    return w3
    
    