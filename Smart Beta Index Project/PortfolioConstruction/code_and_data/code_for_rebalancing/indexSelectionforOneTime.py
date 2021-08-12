import pandas as pd
import numpy as np
import pdb


def index_selection_for_one_time(universe_list,rebalance_date,data_date,
                                 raw_data,
                                 market_cap_weight_df,
                                 total_industry_df,
                                 total_factor_dict,
                                 previous_index,
                                 parameters,
                                 stage):

    #extract data needed for today
    if(stage != 'production_on_review'):
        market_cap_weight_time = market_cap_weight_df.index.asof(rebalance_date) 
    else:
        market_cap_weight_time = market_cap_weight_df.index.asof(data_date)

    if (stage != 'production_on_review'):
        industry_time = total_industry_df.index.asof(rebalance_date) 
    else:
        industry_time = total_industry_df.index.asof(data_date) 

    total_industry = total_industry_df.loc[industry_time,:].to_frame(name='industry').reindex(universe_list)
    original_total_industry = total_industry.copy()
    market_cap_weight = market_cap_weight_df.loc[market_cap_weight_time,:].to_frame(name='marketcap_weight').reindex(universe_list).fillna(0)
    market_cap_weight['marketcap_weight'] = market_cap_weight['marketcap_weight'] / market_cap_weight['marketcap_weight'].sum(skipna=True)
    original_market_cap_weight = market_cap_weight.copy()

    #pdb.set_trace()

    factor_map_dict = parameters['factor_map_dict']
    factor_direction_dict = parameters['factor_direction_dict']
    factor_industry_ignore_dict = parameters['factor_industry_ignore_dict']
    yield_factor_list = parameters['yield_factor_list']
    is_sector_neutral = parameters['is_sector_neutral']
    screening_factor_list = parameters['screening_factor_list']
    screening_delete_percent = parameters['screening_delete_percent']
    screening_buffer_percent = parameters['screening_buffer_percent']
    screening_weights = parameters['screening_weights']

    #generate used factors
    from generateFactorforOneTime import generate_factor_for_one_time
    generated_factor_dict = generate_factor_for_one_time(universe_list,total_factor_dict,data_date,
                                factor_map_dict,factor_direction_dict,factor_industry_ignore_dict,
                                yield_factor_list,
                                total_industry,is_sector_neutral)
    
   

    if(stage!='production_on_capping'):
        
        # for i,j in generated_factor_dict.items():
        #     j.to_excel('rebalance_program_output.xlsx',sheet_name='factor_'+i)

        if(stage=='back-testing'):
            #check on whether there are available factors for today
            for key,factor_df in generated_factor_dict.items():        
                if (np.sum(factor_df['value']!=0)==0) or (np.sum(factor_df['value']!=-3)==0):
                    print('No available factor on rebalance date!'.format(rebalance_date))
                    return market_cap_weight.reindex(universe_list).rename(columns={'marketcap_weight':'weight'})


        #stock screening 
        if parameters['is_stock_screening']:
            from stockScreeningforOneTime import stock_screening_for_one_time
            stock_screened_list = stock_screening_for_one_time(universe_list,total_factor_dict,
                                    data_date , 
                                    screening_factor_list,
                                    total_industry,
                                    is_sector_neutral,
                                    screening_delete_percent,
                                    screening_buffer_percent,
                                    yield_factor_list,
                                    factor_direction_dict,
                                    factor_industry_ignore_dict,
                                    screening_weights,
                                    previous_index)
            universe_list = stock_screened_list
            market_cap_weight = market_cap_weight.reindex(universe_list)
            market_cap_weight['marketcap_weight'] = market_cap_weight['marketcap_weight'] / market_cap_weight['marketcap_weight'].sum(skipna=True)
            total_industry = total_industry.reindex(universe_list)
            print(f'--------length of stock screened universe is {len(stock_screened_list)}--------')
            
            #pdb.set_trace()
            
        # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
        #     pd.DataFrame(index=stock_screened_list).to_excel(writer,sheet_name='stock_screening')



        #dividend screening
        if parameters['is_dividend_screening']:
            from dividendScreeningforOneTime import dividend_screening_for_one_time
            #pdb.set_trace()
            dividend_screened_list = dividend_screening_for_one_time(universe_list, data_date ,previous_index, total_factor_dict['DY_ANN'], 
                                        raw_data['FS_EPS_RPT_DATE'], raw_data['FS_FISCAL_DATE'], raw_data['FS_PAYOUT']) 
                                        #may change to fetch the data insides。

            universe_list = dividend_screened_list
            market_cap_weight = market_cap_weight.reindex(universe_list)
            market_cap_weight['marketcap_weight'] = market_cap_weight['marketcap_weight'] / market_cap_weight['marketcap_weight'].sum(skipna=True)
            total_industry = total_industry.reindex(universe_list)
            print(f'--------length of dividend screened universe is {len(dividend_screened_list)}--------')
            
            #pdb.set_trace()
        # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
        #     pd.DataFrame(index=dividend_screened_list).to_excel(writer,sheet_name='dividend_screening')            
        
        broad_index = universe_list
        
        # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
        #     market_cap_weight.to_excel(writer,sheet_name='marketcap_after_screening') 


        #universe narrowing
        if parameters['is_universe_narrowing']:
            from universeNarrowingforOneTime import universe_narrowing_for_one_time
            narrowed_index = universe_narrowing_for_one_time(universe_list,
                                                            generated_factor_dict,
                                                            market_cap_weight,
                                                            parameters['single_or_multiple_narrowing'],
                                                            parameters['single_factor_name'],
                                                            parameters['narrow_para1'],
                                                            parameters['narrow_para2'],
                                                            parameters['narrow_para3'],
                                                            parameters['factor_add_or_multiplication'],
                                                            parameters['target_function_case_number']
                                                        )

            universe_list = narrowed_index
            market_cap_weight = market_cap_weight.reindex(universe_list)
            market_cap_weight['marketcap_weight'] = market_cap_weight['marketcap_weight'] / market_cap_weight['marketcap_weight'].sum(skipna=True)
            total_industry = total_industry.reindex(universe_list)
            print(f'------length of narrowed universe is {len(narrowed_index)}--------')


        # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
        #     pd.DataFrame(index=narrowed_list).to_excel(writer,sheet_name='universe_narrowing') 


    if(stage == 'production_on_review'):
        return {'broad index':broad_index, 'narrow index':universe_list}

    

    #factor tilting
    from factorTiltforOneTime import factor_tilt_for_one_time
    w1 = factor_tilt_for_one_time(universe_list,generated_factor_dict,
                            market_cap_weight,
                            parameters['factor_add_or_multiplication'],
                            parameters['target_function_case_number'])
    
    # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
    #     w1.to_excel(writer,sheet_name='factor_tilt') 

    #active weight capping (expand the universe)
    if parameters['is_active_weight_capping']:
        from activeWeightCappingforOneTime import active_weight_capping_for_one_time
        w1 = active_weight_capping_for_one_time(original_market_cap_weight,w1,
                                                parameters['min_active_cap'],
                                                parameters['max_active_cap'])                                           
        market_cap_weight = original_market_cap_weight.reindex(w1.index)
        market_cap_weight['marketcap_weight'] = market_cap_weight['marketcap_weight'] / market_cap_weight['marketcap_weight'].sum(skipna=True)
        total_industry = original_total_industry.reindex(w1.index)
        universe_list = w1.index.tolist()
        
    # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
    #     w1.to_excel(writer,sheet_name='active_weight_capping') 
        

    #industry tilting
    from industryTiltforOneTime import industry_tilt_for_one_time
    #pdb.set_trace()
    w2 = industry_tilt_for_one_time(universe_list,original_total_industry,original_market_cap_weight,w1,
                                    parameters['Pj'],parameters['Qj'])

    # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
    #     w2.to_excel(writer,sheet_name='industry_tilting') 


    #final capping
    from finalCappingforOneTime import final_capping_for_one_time
    w3 = final_capping_for_one_time(universe_list,original_market_cap_weight,
                        w2,parameters['max_cap_ratio'],parameters['max_stock_weight'],
                        parameters['is_active_weight_capping'],
                        parameters['min_active_cap'],
                        parameters['max_active_cap'])
    
    # with pd.ExcelWriter('rebalance_program_output.xlsx',engine='openpyxl',mode='a') as writer: 
    #     w3.to_excel(writer,sheet_name='final_capping') 
    

    return w3

