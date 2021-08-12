# -*- coding: utf-8 -*-
"""
Created on Mon Aug 24 10:57:43 2020

@author: Zhihao(Chris)Ren
"""

import numpy as np
import pandas as pd
import sys
import pdb

def stock_screening_for_one_time(universe_list,total_factor_dict,
                                 data_date , 
                                 stock_screening_factor_list,
                                 total_industry,
                                 is_sector_neutral,
                                 stock_screening_delete_percent,
                                 stock_screening_buffer_percent,
                                 yield_factor_list,
                                 factor_direction_dict,
                                 factor_industry_ignore_dict,
                                 stock_screening_weights,
                                 previous_index):
    
    #pdb.set_trace()
    if (len(stock_screening_factor_list)!=len(stock_screening_weights)) or (len(stock_screening_factor_list)==0):
        # print('Stock screening list and weights do not have the same length!')
        raise ValueError('Stock screening list and weights do not have the same length!')

    
    #sub data frames
    Z_dict={}
    
    total_industry = total_industry.reindex(universe_list)

    for f in stock_screening_factor_list:

        factor_df = total_factor_dict[f].reindex(columns=universe_list)
        
        #direction
        if(f in factor_direction_dict.keys()):
            factor_df = factor_df * factor_direction_dict[f] 

        
        #factor time
        factor_time = factor_df.index.asof(data_date)
  
        if pd.isnull(factor_time):
            value_arr_df = pd.DataFrame(np.nan,index=universe_list,columns=['value'])

            
        else:
            value_arr_df = factor_df.loc[factor_time,:].to_frame(name='value')
 
    
        if(is_sector_neutral):
            
            temp_value_arr_df = pd.merge(value_arr_df,total_industry,left_index=True,right_index=True,how='inner')
    
            mean_df = temp_value_arr_df.groupby('industry')['value'].apply(lambda x:x.mean(skipna=True)).reset_index(drop=False)
            std_df = temp_value_arr_df.groupby('industry')['value'].apply(lambda x:x.std(skipna=True)).reset_index(drop=False)
                    

            mean_std_df = pd.merge(mean_df,std_df,on='industry')
                    
            temp_value_arr_df = pd.merge(temp_value_arr_df.reset_index(drop=False),mean_std_df,on='industry',how='inner')
            temp_value_arr_df = temp_value_arr_df.rename(columns={temp_value_arr_df.columns.tolist()[0]:'index'})
            temp_value_arr_df = temp_value_arr_df.set_index('index').reindex(universe_list)
                
            value_arr = ( (temp_value_arr_df['value'] - temp_value_arr_df['value_x']) / temp_value_arr_df['value_y'] ).values
 
        else:
            value_arr = (value_arr_df[f].values-np.nanmean(value_arr_df[f].values))/np.nanstd(value_arr_df[f].values)
    

        if f in yield_factor_list: 
            value_arr[np.isnan(value_arr)] = -3
        else:
            value_arr[np.isnan(value_arr)] = 0
                        
        value_arr[value_arr>3] = 3
        value_arr[value_arr<-3] = -3 
            
        #ignore factor for a particular industry/industries:
        if f in factor_industry_ignore_dict:
            for i in factor_industry_ignore_dict[f]:
                value_arr[total_industry['industry']==i]=np.nan
            

        value_arr_df['value']=value_arr
            
        
        Z_dict[f]=value_arr_df
        
        
   
    #average z value
    total_value_df = Z_dict[stock_screening_factor_list[0]]
    
    # pdb.set_trace()
    
    # with pd.ExcelWriter('rebalance_manual_test.xlsx',engine='openpyxl',mode='a') as writer: 
    #     for i,j in Z_dict.items():
    #         j.to_excel(writer,sheet_name='screening_factor_'+i)
            
    
    for f in stock_screening_factor_list[1:]:
        total_value_df = pd.merge(total_value_df , Z_dict[f] , left_index=True, right_index=True) 

    
    total_value_df = total_value_df.apply(lambda x:x.values.dot(np.array(stock_screening_weights)),axis=1).\
        to_frame(name='value').sort_values(by='value',ascending=True)
    
    #screening w.r.t. industry
    merged_df = pd.merge(total_value_df,total_industry,left_index=True,right_index=True)
    
    def for_grp(x):

        x=x.sort_values(by='value',ascending=True)
        
        #buffer zone
        if len(previous_index)==0:
            final_list_x = x.iloc[max(1,round(stock_screening_delete_percent*len(x))):].index.tolist()
        else:  
            sorted_univ_list = x.index.tolist()
            w0_merged = pd.merge(pd.DataFrame(index=previous_index),total_industry,left_index=True,right_index=True)
            w0_merged = w0_merged[w0_merged['industry']==x['industry'][0]]
            current_univ_list = list(set(w0_merged.index.tolist()).intersection(universe_list)) 
            out_list = list(set(current_univ_list).intersection(sorted_univ_list[:max(1,round((stock_screening_delete_percent-\
                                                                stock_screening_buffer_percent) * len(sorted_univ_list)))]))
            in_list = list(set(sorted_univ_list[max(1,round((stock_screening_delete_percent+stock_screening_buffer_percent)*\
                                                    len(sorted_univ_list))):]).difference(current_univ_list))
            final_list_x = [x for x in current_univ_list if x not in out_list] + in_list
            
        return final_list_x
        
    if len(previous_index)==0:
        print('We do not have the last rebalancing weight, do not implement buffer zone')
    
    final_list = []
    for i in merged_df.groupby('industry').apply(for_grp).values:
        final_list+=i 
    
    screened_index = sorted([universe_list.index(i) for i in final_list])
    screened_list = [universe_list[i] for i in screened_index]

    
    return screened_list