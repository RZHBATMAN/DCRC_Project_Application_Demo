# -*- coding: utf-8 -*-
"""
Created on Tue Aug 18 15:48:40 2020

@author: Zhihao(Chris)Ren
"""


# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 13:02:43 2020

@author: Zhihao(Chris)Ren
"""


import pandas as pd
import numpy as np
from scipy.stats import norm
import math
import pdb
 

def factor_tilt_for_one_time(universe_list,generated_factor_dict,
                             market_cap_weight,is_factor_add_or_multiplication,target_function_case_number):
    
 
    from sScoreforOneTime import s_score_for_one_time
    s_df_for_this_time = s_score_for_one_time(universe_list,
                        generated_factor_dict,
                        market_cap_weight,
                        is_factor_add_or_multiplication,
                        target_function_case_number)
    

    #calculate weight 
    original_weight_dict = dict(zip(market_cap_weight.index,market_cap_weight['marketcap_weight'].values))
   

    weight_dict = original_weight_dict.copy()
    
    for stock in weight_dict.keys():
        this_weight = weight_dict[stock]
        idx=0
        for i in range(len(generated_factor_dict)):
            key = list(generated_factor_dict.keys())[i]
            if(is_factor_add_or_multiplication=='multiplication'):
                this_weight*=s_df_for_this_time.loc[stock,key] 
            else:
                this_weight+=s_df_for_this_time.loc[stock,key]
            idx+=1
            if(np.isnan(this_weight)):
                this_weight=0
        weight_dict[stock] = this_weight
  
    w1 = pd.DataFrame.from_dict(weight_dict,orient='index',columns=['weight'])
    
    
    w1 = w1.reindex(index=universe_list)
    w1.fillna(value=0,inplace=True)
    w1['weight'] = w1['weight'] / w1['weight'].sum() #standardize

  
    return w1
            
    
    