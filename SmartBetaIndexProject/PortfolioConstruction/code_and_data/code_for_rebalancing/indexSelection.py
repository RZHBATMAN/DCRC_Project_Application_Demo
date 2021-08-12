# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 16:25:43 2020

@author: Zhihao(Chris)Ren
"""

import json
import pdb

import pandas as pd
import numpy as np

from db import DB
from indexSelectionforOneTime import index_selection_for_one_time


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

# TODO: which one of 3 dates rebalance_date is?
def save_diary(db, fac_construct_id, index_id, parameter_id,
               impl_date, fac_data_date, capping_date, review_date, 
               info, index, rebal_weight, factors, raw_data):
    rebal_id = db.gen_rebal_id(
                fac_construct_id,
                parameter_id,
                index_id,
                impl_date,
                fac_data_date,
                capping_date,
                review_date,
                info)

    db.save_rebal_weights(rebal_weight, 'dummy', rebal_id)   # TODO: see what role name plays
    db.save_fac_snapshots(factors, rebal_id)
    db.save_index_snapshots(index, rebal_id)
    db.save_raw_snapshots(raw_data, -rebal_id)   # we use negative rebal_id to indicate its a rebal_id (instead of fac_construct_id)


def index_selection(rebalance_dates = pd.NaT,
                    data_dates = pd.NaT, 
                    hyper_parameters = {},
                    parameters = {},
                    stage = 'back_testing', 
                    # three choices: 'back_testing' , 'life_production_on_review', 'life_production_on_capping'
                    previous_index = [],
                    options = None):
    
    '''
    For back-testing, inputs:
        universe_list_dict, rebalance dates, data dates, parameter_id (parameters), factor_construction_id,
        data_fetch_hyper_parameter
        (load dataframe from database inside)
    Output: rebalance dataframe (may transform to csv later)
        

    For production, on review date, for client: inputs:
        universe_list_dict, data_date, parameter_id, 
        data_fetch_hyper_parameter
        (read market_cap and industry from csv)
    Output: broad index, narrowed list
    

    For production, on capping date, for client: inputs:
        narrowed list, parameter_id, industry, csv index weights,
        data_fetch_hyper_parameter
        (read market_cap and industry from csv)
    Output: rebalancing weights
    

    Do another thing: Save production rebalancing information to database (rebalance tables)

    '''
    
    stages = ['back_testing','production_on_review','production_on_capping']

    if stage not in stages:
        print('The input command for stages is incorrect!')    

    #check inputs, only for back-testing
    if stage == 'back_testing':
        if len(rebalance_dates)!=len(data_dates):
            raise ValueError('The input rebalance and data dates do not match!')
        
        rebalance_dates = [pd.Timestamp(i) for i in rebalance_dates]
        data_dates = [pd.Timestamp(i) for i in data_dates]
        
        

    else:   # now must be production
        input_df = pd.read_csv(hyper_parameters[f'{stage}_input_file'])
        data_dates = [pd.Timestamp(input_df.loc[0,'data_date'])]
        rebalance_dates = [pd.Timestamp(input_df.loc[0,'rebalance_date'])]
        market_cap_weight_df = input_df.set_index("RIC_Code")[['Wgt']].T.fillna(0)
        total_industry_df = input_df.set_index("RIC_Code")[['Industry']].T.fillna(-5)

        if stage == 'production_on_review':
            market_cap_weight_df.index = data_dates
            total_industry_df.index = data_dates
            universe_list_dict = {rebalance_dates[0]:input_df['RIC_Code'].tolist()}
        elif stage == 'production_on_capping':
            market_cap_weight_df.index = rebalance_dates
            total_industry_df.index = rebalance_dates
            df = pd.read_csv(hyper_parameters['production_on_review_output_file'])
            universe_list_dict = {rebalance_dates[0]:df[hyper_parameters['broad_or_narrow_index']].tolist()}
        else:  # to disable those possible unbounded variables warnings
            raise ValueError
    
    db_params = hyper_parameters['db']
    db = DB(db_params['work_directory'], db_params['user'], db_params['password'])  # TODO: user enter password!
    
    with db:  
        #deal with hyperparameters and parameters
        temp_parameters = db.load_parameter(hyper_parameters['parameter_set_name'])


        if 'screening_factor_list' in parameters.keys() and parameters['screening_factor_list']:
            temp_parameters['screening_weights']=np.repeat(1/len(parameters['screening_factor_list']),
                                                                 len(parameters['screening_factor_list']))
        else:
            temp_parameters['screening_weights'] = np.array([])

        for i in temp_parameters:
            if i in parameters.keys():
                temp_parameters[i]=parameters[i]
        
        parameters = temp_parameters

        startdate = f'{data_dates[0]-pd.Timedelta(days=1200):%Y-%m-%d}'
        enddate   = f'{data_dates[-1]:%Y-%m-%d}'
        
        #pdb.set_trace()

        all_hk_stock_list = sorted(db.get_insts(startdate,enddate,exchange='HK')) 
        
        #or all_hk_stock_list = sorted(db.get_insts(startdate,enddate,'univ_hslmic')) 
        
        
        
        raw_data = db.load_raw(hyper_parameters['raw_data_needed'], startdate, enddate, all_hk_stock_list)
        fac_list = parameters['screening_factor_list'] + list(parameters['factor_map_dict'].values())[0] + ['DY_ANN']
        fac_list= list(set(fac_list))
        
        #this only works for single factor index
        total_factor_dict = db.load_fac(fac_list,(data_dates[0]-pd.Timedelta(days=1200)).strftime('%Y-%m-%d'),
                                        (data_dates[-1]+pd.Timedelta(days=1200)).strftime('%Y-%m-%d'),all_hk_stock_list)
        
        #pdb.set_trace()
        if(stage == 'back_testing'):
            total_industry_df = list(db.load_raw('HS_IND',(rebalance_dates[0]-pd.Timedelta(days=50)).strftime('%Y-%m-%d'),
                                 (rebalance_dates[-1]+pd.Timedelta(days=50)).strftime('%Y-%m-%d'),all_hk_stock_list).values())[0]
            
            market_cap_weight_df  = pd.merge_asof(pd.DataFrame(index=rebalance_dates), 
                                                  list(db.load_univ(hyper_parameters['universe_name'],
                                                      (rebalance_dates[0]-pd.Timedelta(days=100)).strftime('%Y-%m-%d'),
                                                      (rebalance_dates[-1]+pd.Timedelta(days=100)).strftime('%Y-%m-%d'),
                                                      all_hk_stock_list).values())[0],
                                                  left_index=True,right_index=True,direction='nearest').fillna(0)
    
    

    #what do you want to do
    if stage == 'back_testing':

        rebalance_df = pd.DataFrame(0, index = rebalance_dates, columns = all_hk_stock_list)
    
        #conduct rebalancing
        for i,rebalance_date in enumerate(rebalance_dates):

            data_date = data_dates[i]
            print('\n')
            print('..........the rebalance date is {}........'.format(rebalance_date))
            
            #universe_list = universe_list_dict[rebalance_date]
            
            with db:
                universe_list = db.get_insts(f'{rebalance_date:%Y-%m-%d}',f'{rebalance_date:%Y-%m-%d}','univ_hslmic')
            
            #pdb.set_trace()
            
            w_final = index_selection_for_one_time(universe_list,rebalance_date,data_date,
                                                raw_data,market_cap_weight_df,
                                                total_industry_df,
                                                total_factor_dict,
                                                previous_index,
                                                parameters,
                                                stage)

            rebalance_df.loc[rebalance_date,:] = w_final['weight'].reindex(all_hk_stock_list).fillna(0)
        
            previous_index = w_final.index.tolist()

            print('\n')
        
        #save back_testing diary
        index = {hyper_parameters['universe_name']: market_cap_weight_df}
        raw_data = {**raw_data, 'HS_IND': total_industry_df}
        with db:
            fac_construct_id = db.get_fac_construct_id() # TODO: may passed by user
            index_id = db.get_index_id( next((i for i in index.keys())) ) 
            
            fac_data_dates = data_dates
            impl_date,  capping_date, review_dates  = pd.Timestamp.today(), pd.Timestamp.today(),rebalance_dates  

            rebal_weight = w_final.T
            rebal_weight.index = [impl_date]
            

            info = json.dumps({'parameters': parameters, 'hyper_parameters': hyper_parameters}, cls=NumpyEncoder)


            save_diary(db, fac_construct_id, index_id, parameters['id'],
                       impl_date, fac_data_dates[-1], capping_date, review_dates[-1], 
                       'This is a back-history. ' + info, index, rebal_weight, total_factor_dict, raw_data)
            #TODO: Need to save all the dates and any other information that can replicate this back history, to the 'info'
        
        rebalance_df.to_csv(hyper_parameters["backtesting_output_file"],index=True)
        
        return rebalance_df
    
        
        
        

    
    else:
        if (len(universe_list_dict)!=1) or (len(rebalance_dates)!=1) or (len(data_dates)!=1):
            raise ValueError('lengths of univese_list_dict, rebalance_dates, and data_dates must be 1')

        universe_list = list(universe_list_dict.values())[0]
        rebalance_date = rebalance_dates[0]
        data_date = data_dates[0]
        
        life_res = index_selection_for_one_time(
                                 universe_list,rebalance_date,data_date,
                                 raw_data,
                                 market_cap_weight_df,
                                 total_industry_df,
                                 total_factor_dict,
                                 previous_index,
                                 parameters,
                                 stage)


        # review_date, capping_date = None, None
        # if stage == 'production_on_review':
        #     review_date = rebalance_date
        # elif stage == 'production_on_capping':
        #     capping_date = rebalance_date



        #save production diary
        index = {hyper_parameters['universe_name']: market_cap_weight_df}
        raw_data = {**raw_data, 'HS_IND': total_industry_df}
        if stage == 'production_on_capping':
            
            with db:
                fac_construct_id = db.get_fac_construct_id() # TODO: may passed by user
                index_id = db.get_index_id( next((i for i in index.keys())) ) 
                fac_data_date = data_date
                impl_date = rebalance_date # TODO
    
                rebal_weight = life_res.T
                rebal_weight.index = [impl_date]
                
                # capping_date = rebalance_date    # TODO: remove this
                info = json.dumps({'parameters': parameters, 'hyper_parameters': hyper_parameters}, cls=NumpyEncoder)

                
                review_date = pd.Timestamp(hyper_parameters['review_date'])
                capping_date = pd.Timestamp(hyper_parameters['capping_date'])
    
                save_diary(db, fac_construct_id, index_id, parameters['id'],
                            impl_date, fac_data_date, capping_date, review_date, 
                            info, index, rebal_weight, total_factor_dict, raw_data)
                
        
        #output csv results
        if stage == 'production_on_review':
            
            output_df = pd.DataFrame({'review date':np.repeat(pd.Timestamp(hyper_parameters['review_date']),len(life_res['broad index'])),
                                      'index name':np.repeat(hyper_parameters['index_name'],len(life_res['broad index'])),
                                      'broad index':life_res['broad index'],
                                      'narrow index':life_res['narrow index'] + list(np.repeat(np.nan,len(life_res['broad index'])
                                                                                               -len(life_res['narrow index'])))
                                      })

            
        else:
            
            life_res.index.name=hyper_parameters['broad_or_narrow_index']
            output_df = life_res.reset_index(drop=False)
            output_df.insert(0,'capping date',pd.Timestamp(hyper_parameters['capping_date']))
        
        output_df.to_csv(hyper_parameters[f'{stage}_output_file'],index=False)

           
        
        return life_res


