# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 16:45:57 2020

@author: Zhihao(Chris)Ren
"""

import pandas as pd
import numpy as np
import pdb

def get_total_factor_dict(universe,Factor_dict,
                          rebalance_dates,data_dates,tracker_dates,
                          factor_map_dict,factor_direction_dict,
                          total_industry_df,
                          is_sector_neutral,
                          market_cap_weight_df,
                          factor_industry_ignore_dict):
    
    """
    
    Parameters
    ----------
    universe: dictionary
        contain the information of the universe.
    Factor_dict: dictionary
        contain the original usable factors
    rebalance_dates: list
        the list of re-balance dates 
    factor_map_dict: dictionary
        the dictionary mapping the general Barra factors to the sub-factors
    factor_direction_dict: dictionary
        indicate the directory of sub-factors, the values are either 1 or -1
        
    Returns
    -------
    total_factor_dict: dictionary
        the dictionary containing the used factors, which will be used in portfolio rebalance.

    """         
    
   
    #standardize all factors except beta, deal with missing data

    total_factor_dict = {}
    
    for f in factor_map_dict: # dict {'beta':['capm'], 'v': ['sadd']}
         
        df_for_f = pd.DataFrame(np.nan,index = data_dates,columns = universe['bmhd'].columns)
        
  
        for i,t in enumerate(rebalance_dates):
            
            value_arr_df = pd.DataFrame(np.nan,index = universe['bmhd'].columns,columns = factor_map_dict[f]) #index is stock code
            #industry_time = total_industry_df.index.asof(t)
            factor_time = data_dates[i]
            industry_time = data_dates[i]
            tracker_time = tracker_dates[i]
            this_industry_df = total_industry_df.loc[industry_time,:].to_frame(name='industry').reindex(universe['bmhd'].columns)
            
            
        
            for sub_f in factor_map_dict[f]:
            
                this_factor_df = Factor_dict[sub_f].reindex(columns = universe['bmhd'].columns)
                
                # if(t==rebalance_dates[1]):
                
                #     pdb.set_trace()
            
                """
                determine the time of factors (cannot predict the future)
            
                Here, we use half-year factor
        
                """ 
                # if(t.month<=6):
                #     chose =  this_factor_df[np.logical_and(this_factor_df.index.year==t.year-1,
                #                                              this_factor_df.index.month==12)].index
            
            
                # else:
                #     chose =  this_factor_df [np.logical_and(this_factor_df.index.year==t.year,
                #                                              this_factor_df.index.month==6)].index
                
                
                # if(len(chose)==0):
                #     print("------------ the re-balancing date is {} ------------".format(t))
                #     print('there is no proper value for the factor {}, so set it to 0'.format(sub_f))
                #     temp_value_arr = np.zeros(universe['bmhd'].shape[1])
                #     temp_value_arr[:] = np.nan  
                # else:
                #     factor_time = chose[-1] #
                #     temp_value_arr = this_factor_df.loc[factor_time,:].values
                
                
                if(factor_time not in this_factor_df.index.tolist()):
                     print("------------ the re-balancing date is {} ------------".format(t))
                     print('there is no proper value for the factor {}, so set it to 0'.format(sub_f))
                     temp_value_arr = np.zeros(universe['bmhd'].shape[1])
                     temp_value_arr[:] = np.nan  
                else:
                    temp_value_arr = this_factor_df.loc[factor_time,:].values

                
                    
                # initial step: get rid of all 0-weight factors in market cap
                market_cap_arr = market_cap_weight_df.loc[tracker_time,:].values
                temp_value_arr[market_cap_arr == np.float64(0)]=np.nan
                
                # if(t==rebalance_dates[1]):
                #     pdb.set_trace()
                    
                #direction  
                if(sub_f in factor_direction_dict.keys()):
                    temp_value_arr = temp_value_arr*factor_direction_dict[sub_f]  

  
                value_arr_df.loc[:,sub_f] = temp_value_arr  
                
              
                #z score for sub-factor
                if(is_sector_neutral):

                    temp_value_arr_df = pd.merge(value_arr_df,this_industry_df,left_index=True,right_index=True,how='inner')
    
                    mean_df = temp_value_arr_df.groupby('industry')[sub_f].apply(lambda x:x.mean(skipna=True)).reset_index(drop=False)
                    std_df = temp_value_arr_df.groupby('industry')[sub_f].apply(lambda x:x.std(skipna=True)).reset_index(drop=False)
                    

                    mean_std_df = pd.merge(mean_df,std_df,on='industry')
                    
                    temp_value_arr_df = pd.merge(temp_value_arr_df.reset_index(drop=False),mean_std_df,on='industry',how='inner')
                    temp_value_arr_df = temp_value_arr_df.set_index('index').reindex(universe['bmhd'].columns)
                
                    value_arr = (temp_value_arr_df[sub_f].values - temp_value_arr_df[sub_f+'_x'].values) / temp_value_arr_df[sub_f+'_y'].values
                
                    # value_arr = factor_df['standardized_value'].values
                    
                    # if(t==rebalance_dates[1]):
                    #     pdb.set_trace()
                
                else:
                    value_arr= (value_arr_df[sub_f].values-np.nanmean(value_arr_df[sub_f].values))/np.nanstd(value_arr_df[sub_f].values)
                        
                # if(t==rebalance_dates[2]):
                #     pdb.set_trace()
                    
                # fill the missing value 
                if(f=='Y'): # fill the missing value of 'yield'
                    value_arr[np.isnan(value_arr)] = -3
                else:
                    value_arr[np.isnan(value_arr)] = 0
                        
                value_arr[value_arr>3] = 3
                value_arr[value_arr<-3] = -3 
                
                value_arr[market_cap_arr == np.float64(0)]=0 #assure the values are 0 for 0-cap weight stocks 
                
                
                #pdb.set_trace()
                #ignore this factor according to industry
                if (len(factor_industry_ignore_dict)!=0) and (sub_f in factor_industry_ignore_dict):
                    for i in factor_industry_ignore_dict[sub_f]:
                        value_arr[this_industry_df['industry']==i]=np.nan
                    
                
                
                value_arr_df[sub_f] = value_arr
                
                
             

            #aggregate to the factor's value
            value_arr_df = value_arr_df.mean(skipna=True,axis=1).to_frame(name='value')

            df_for_f.loc[t,:] = value_arr_df['value'].values
    
        total_factor_dict[f] = df_for_f
        

        
    
    return total_factor_dict # each within the dict is w.r.t. re-balance dates, with missing values dealt with