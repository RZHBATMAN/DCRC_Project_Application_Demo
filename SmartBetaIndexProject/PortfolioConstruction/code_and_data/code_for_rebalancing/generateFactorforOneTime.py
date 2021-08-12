import pandas as pd
import numpy as np
import pdb

def generate_factor_for_one_time(universe_list,total_factor_dict,data_date,
                                 factor_map_dict,factor_direction_dict,factor_industry_ignore_dict,
                                 yield_factor_list,
                                 total_industry,is_sector_neutral):

 

    generated_factor_dict = {}

    for f in factor_map_dict: 

        value_arr_df = pd.DataFrame(np.nan,index = universe_list, columns = factor_map_dict[f]) 

        for sub_f in factor_map_dict[f]:
            
            this_factor_df = total_factor_dict[sub_f].reindex(columns = universe_list)

            factor_time = this_factor_df.index.asof(data_date)    
 
            if(pd.isnull(factor_time)):
                print(f'There is no proper value for the factor {sub_f} on data date {data_date}')
                temp_value_arr = np.zeros(len(universe_list))
                temp_value_arr[:] = np.nan  
            else:
                temp_value_arr = this_factor_df.loc[factor_time,:].values

     
            #direction  
            if(sub_f in factor_direction_dict.keys()):
                temp_value_arr = temp_value_arr*factor_direction_dict[sub_f]  

            value_arr_df.loc[:,sub_f] = temp_value_arr  
              
            #z score for sub-factor
            if(is_sector_neutral):
                temp_value_arr_df = pd.merge(value_arr_df,total_industry,left_index=True,right_index=True,how='inner')
                mean_df = temp_value_arr_df.groupby('industry')[sub_f].apply(lambda x:x.mean(skipna=True)).reset_index(drop=False)
                std_df = temp_value_arr_df.groupby('industry')[sub_f].apply(lambda x:x.std(skipna=True)).reset_index(drop=False)
        
                mean_std_df = pd.merge(mean_df,std_df,on='industry')    
                temp_value_arr_df = pd.merge(temp_value_arr_df.reset_index(drop=False),mean_std_df,on='industry',how='inner')
                temp_value_arr_df = temp_value_arr_df.rename(columns={temp_value_arr_df.columns.tolist()[0]:'index'})
                temp_value_arr_df = temp_value_arr_df.set_index('index').reindex(universe_list)
                
                value_arr = ( (temp_value_arr_df[sub_f] - temp_value_arr_df[sub_f+'_x']) / temp_value_arr_df[sub_f+'_y'] ).values  
                
            else:
                value_arr= (value_arr_df[sub_f].values-np.nanmean(value_arr_df[sub_f].values))/np.nanstd(value_arr_df[sub_f].values)
      
            #fill the missing value 
            if(f=='Y') or (sub_f in yield_factor_list): 
                value_arr[np.isnan(value_arr)] = -3
            else:
                value_arr[np.isnan(value_arr)] = 0
                        
            value_arr[value_arr>3] = 3
            value_arr[value_arr<-3] = -3 

            #ignore factor for a particular industry/industries:   
            if sub_f in factor_industry_ignore_dict:
                for i in factor_industry_ignore_dict[sub_f]:
                    value_arr[total_industry['industry']==i]=np.nan
                    

            value_arr_df[sub_f] = value_arr

        #aggregate to the factor's value
        df_for_f = value_arr_df.mean(skipna=True,axis=1).to_frame(name='value') #today's factor value

        generated_factor_dict[f] = df_for_f
        

    return generated_factor_dict