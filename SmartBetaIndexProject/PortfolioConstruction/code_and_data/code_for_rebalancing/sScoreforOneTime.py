# -*- coding: utf-8 -*-
"""
Created on Thu Aug 20 15:41:06 2020

@author: Zhihao(Chris)Ren
"""
import pandas as pd
import numpy as np
import sys
from scipy.optimize import fsolve 

def s_score_for_one_time(universe_list,
                        generated_factor_dict,
                        market_cap_weight,
                        is_factor_add_or_multiplication,
                        target_function_case_number):
    
     
    s_df_for_this_time = pd.DataFrame(np.nan,index=universe_list,columns=list(generated_factor_dict.keys()))
    
    if(target_function_case_number==0):  #do nothing
        for f in generated_factor_dict:
            factor_df = generated_factor_dict[f].reindex(universe_list)
            s_value = factor_df['value'].values
            s_df_for_this_time.loc[:,f]=s_value
     
     
    elif (target_function_case_number==1): #math.exp
        import math
        for f in generated_factor_dict:
            factor_df = generated_factor_dict[f].reindex(universe_list)
            s_value = factor_df['value'].apply(lambda x:math.exp(x)).values
            s_df_for_this_time.loc[:,f]=s_value
 

    elif (target_function_case_number==2): #norm.cdf
        from scipy.stats import norm
        for f in generated_factor_dict:
            factor_df = generated_factor_dict[f].reindex(universe_list)
            s_value  = factor_df['value'].apply(lambda x:norm.cdf(x)).values
            s_df_for_this_time.loc[:,f]=s_value
 
 
    elif (target_function_case_number==3): # 1+x and 1/(1+|x|)
        for f in generated_factor_dict:
            factor_df = generated_factor_dict[f].reindex(universe_list)
            s_value  = factor_df['value'].apply(lambda x : 1+x if x>=0 else 1/(1+np.abs(x))).values
            s_df_for_this_time.loc[:,f]=s_value
 
 
    elif (target_function_case_number==4): # 1+x and 1/(1+|x|), need to solve for s

        original_weight_dict = dict(zip(market_cap_weight.index,market_cap_weight['marketcap_weight'].values))
    
        def solve_for_s(s): 
    
            target_factor_function = lambda x : 1+x/s if x>=0 else 1/(1+np.abs(x)*s)
    
            #calculate weight
            copy_weight_dict = original_weight_dict.copy()
            for stock in copy_weight_dict.keys():
                this_weight = copy_weight_dict[stock]
                idx=0
                for i in range(len(generated_factor_dict)):
                    key = list(generated_factor_dict.keys())[i]
                    factor_df = generated_factor_dict[key]
                    if(is_factor_add_or_multiplication=='multiplication'):
                        this_weight*=target_factor_function(factor_df.loc[stock,'value'])#take into account the strength
                    else:
                        this_weight+=target_factor_function(factor_df.loc[stock,'value'])
                    idx+=1
                    
                if np.isnan(this_weight):
                    this_weight=0
                copy_weight_dict[stock] = this_weight
        
        
            return np.sum(list(copy_weight_dict.values()))-1 
      
        #solve for s 
        s_solved = fsolve(solve_for_s,0.8)[0]
    
        for f in generated_factor_dict:
            factor_df = generated_factor_dict[f].reindex(universe_list)
            s_value  = factor_df['value'].apply(lambda x : 1+x/s_solved if x>=0 else 1/(1+np.abs(x)*s_solved)).values
            s_df_for_this_time.loc[:,f]=s_value
 
    else:
        print('.................... case number incorrect! .......................')
        sys.exit()



    return s_df_for_this_time 