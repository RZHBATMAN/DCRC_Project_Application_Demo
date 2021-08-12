# -*- coding: utf-8 -*-
"""
Created on Fri Aug  7 17:00:25 2020

@author: Zhihao(Chris)Ren
"""
import pandas as pd
import numpy as np
import sys
import pdb

def universe_narrowing_for_one_time(universe_list,
                                    generated_factor_dict,
                                    market_cap_weight,
                                    is_single_or_multiple_narrowing,
                                    single_factor_name,
                                    narrow_para1,narrow_para2,narrow_para3,
                                    is_factor_add_or_multiplication,
                                    target_function_case_number
                                    ):
    
       
    
    
    if(is_single_or_multiple_narrowing=='single'):

        from sScoreforOneTime import s_score_for_one_time
        factor_df = s_score_for_one_time(universe_list,
                        {single_factor_name:generated_factor_dict[single_factor_name]},
                        market_cap_weight,
                        is_factor_add_or_multiplication,
                        target_function_case_number)

        
        factor_df = factor_df.rename(columns={single_factor_name:'factor_value'})
        
        
        info_df = pd.DataFrame({'marketcap_weight':market_cap_weight['marketcap_weight'].values,
                                'factor_value':factor_df['factor_value'].values,
                                'factor_exposure':market_cap_weight['marketcap_weight'].values * factor_df['factor_value'].values},
                                 index=universe_list).sort_values(by='factor_exposure',ascending=False)   
        
        #pdb.set_trace()
        
    else:
    
        print('have not done universe narrowing on multiple factors yet')
        sys.exit()

        
    #conduct narrowing
    narrowed_list = info_df.index[:int(info_df.shape[0]*narrow_para3)]
    narrowed_index = sorted([universe_list.index(i) for i in narrowed_list])
    narrowed_list = [universe_list[i] for i in narrowed_index]
    
    return narrowed_list