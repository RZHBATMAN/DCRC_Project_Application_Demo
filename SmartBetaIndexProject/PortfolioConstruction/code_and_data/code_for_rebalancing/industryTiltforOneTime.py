# -*- coding: utf-8 -*-
"""
Created on Wed Aug  5 12:02:30 2020

@author: Zhihao(Chris)Ren
"""
import pandas as pd
import numpy as np
from scipy.optimize import fsolve 
import pdb

def industry_tilt_for_one_time(universe_list,original_total_industry,original_market_cap_weight,w1,Pj,Qj):
    

    
 
    #original_total_industry = original_total_industry.reindex(universe_list)

    info_df = pd.merge(w1,original_market_cap_weight,left_index=True,right_index=True,how='right').fillna(0)
    info_df = pd.merge(info_df,original_total_industry,left_index=True,right_index=True,how='inner')    
    
    
    
    industry_low = info_df.groupby('industry').apply(lambda x:max((1-Pj)*x['marketcap_weight'].sum(skipna=True)-Qj,0)).to_frame(name='industry_lower')
    low_by_industry = industry_low['industry_lower'].values
    industry_high = info_df.groupby('industry').apply(lambda x:min((1+Pj)*x['marketcap_weight'].sum(skipna=True)+Qj,1)).to_frame(name='industry_higher')
    high_by_industry = industry_high['industry_higher'].values


    weights_by_industry = info_df.groupby('industry')['weight'].sum().values 
    weights_by_industry[weights_by_industry<low_by_industry] = low_by_industry[weights_by_industry<low_by_industry]
    weights_by_industry[weights_by_industry>high_by_industry] = high_by_industry[weights_by_industry>high_by_industry]
    
    

    industry_constraint = pd.DataFrame({'industry_constraint':weights_by_industry},index=industry_low.index)
    

    info_df = pd.merge(info_df,industry_constraint,left_on='industry',right_index=True,how='inner')
    
    j = industry_constraint.shape[0]
    


    
    
    #solve the equations 
    def solve_for_coefficient(coef): #coef is of length j: the number of industry

        industry_coef = pd.DataFrame({'industry_coef':coef},index=industry_low.index)
        
        copy_info_df = info_df.copy()
        
    
        copy_info_df = pd.merge(copy_info_df,industry_coef,left_on='industry',right_index=True,how='inner')
        copy_info_df['w2']=copy_info_df['weight']*copy_info_df['industry_coef'] #*copy_info_df['country_coef']
        copy_info_df['w2'] = copy_info_df['w2']/copy_info_df['w2'].sum()
        
        returned_list_2 = copy_info_df.groupby('industry').apply(lambda x:x['w2'].sum()-x['industry_constraint'].values[0]).values.tolist()
        
        copy_info_df.groupby('industry').apply(lambda x:x['w2'].sum())
        
        return returned_list_2 
    
    
    solved_coef = fsolve(solve_for_coefficient,np.repeat(0.8,j))      
    

    industry_coef = pd.DataFrame({'industry_coef':solved_coef},index=industry_low.index)


    info_df = pd.merge(info_df,industry_coef,left_on='industry',right_index=True,how='inner')
    info_df['weight']=info_df['weight']*info_df['industry_coef']
    
    w2 = info_df[['weight']]
    

    w2 = w2.reindex(index=universe_list)
    w2.fillna(value=0,inplace=True)
    w2['weight'] = w2['weight'] / w2['weight'].sum()
    
    return w2