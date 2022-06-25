# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

###there are 8 parts of this code requrie manual changes related to which date is it 

import pandas as pd
import numpy as np

##############################
##part 0 - read raw data
##############################

##mapping and lookup table
shipper_node_id_mapping = pd.read_csv(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\input file\shipper_node_id_mapping.csv')

node_summary_lookup = pd.read_csv(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\input file\node_summary_lookup.csv')

sc_summary_lookup = pd.read_csv(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\input file\sc_summary_lookup.csv')


#===========================================
# 8) change the adjustor - to source the latest data 
#######################################

##output from LOM and Sai
# below change daily
lom_result = pd.read_csv(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\input file\2022-06-08\SWA_UK_Pickup_Forecast_PROD-REACTIVE-DAY_BEFORE_at_2022-06-07-16 22 06-UTC_from_2022-06-08_to_2022-06-08.csv')

#below only change weekly sun-sat next week
shipper_forecast_week= pd.read_excel(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\input file\2022-06-08\Shipper forecast week 23.xlsx', sheet_name='shipper_forecast_week')
weekly_forecast_swa= pd.read_excel(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\input file\2022-06-08\Weekly Forecast SWA Wk 23 Maxime.xlsx')



##########################################################
##part 1 - take raw data from LOM report result and shipper forecast weekly 
###########################################################
def transform_lom_report(df):    
    SWA_UK_Pickup_Forecast = df[['ForecastWeek', 'ForDate', 'ForecastDayOfWeek', 'MerchantId',
                                    'ForecastAccountId', 'StopId', 'ProgramType', 'ServiceType',
                                    'ShipperName', 'WarehouseName','NodeId', 'RoutingPrefix',
                                    'Original Forecast Pallet Count','Original Forecast Volume (cubic meters)','Original Forecast Shipment Count']]

    SWA_UK_Pickup_Forecast.rename(columns = {'Original Forecast Pallet Count':'Today\'s Forecast (LOM) Pallet Count', 
                                         'Original Forecast Volume (cubic meters)':'Today\'s Forecast (LOM) Volume (cubic meters)',
                                         'Original Forecast Shipment Count':'Actual LOM'
                                         },inplace = True)

#===========================================
# 1) change the adjustor - Uncapped Lom could
    SWA_UK_Pickup_Forecast['Uncapped LOM'] = round(SWA_UK_Pickup_Forecast['Actual LOM'] *1)
#===========================================

    SWA_UK_Pickup_Forecast['Today\'s Forecast (LOM) Shipment Count'] = SWA_UK_Pickup_Forecast['Uncapped LOM']
    SWA_UK_Pickup_Forecast['Today\'s Forecast (LOM) SPP'] = SWA_UK_Pickup_Forecast['Today\'s Forecast (LOM) Shipment Count']/SWA_UK_Pickup_Forecast['Today\'s Forecast (LOM) Pallet Count']


##read data from shipper forecast weekly 
    SWA_UK_Pickup_Forecast= pd.merge(SWA_UK_Pickup_Forecast,shipper_node_id_mapping,
                                  how = 'left',left_on ='NodeId',right_on ='NodeId')
    SWA_UK_Pickup_Forecast.rename(columns = {'Node':'Sort Forecast'},inplace = True)
  
#==========================================
# 2) change the adjustor - which day of the forcast
    shipper_forecast = shipper_forecast_week[['Stop ID', 'Merchant Id','Wed Forecast']]
    shipper_forecast.rename(columns = {'Wed Forecast':'S&OP Forecast Shipment Count'},inplace = True)
#============================================================                                       

    SWA_UK_Pickup_Forecast= pd.merge(SWA_UK_Pickup_Forecast,shipper_forecast,
                                     how = 'left',left_on ='StopId',right_on ='Stop ID')

    SWA_UK_Pickup_Forecast['LOM vs S&OP shipment count']= SWA_UK_Pickup_Forecast['Today\'s Forecast (LOM) Shipment Count'] - SWA_UK_Pickup_Forecast['S&OP Forecast Shipment Count']
    SWA_UK_Pickup_Forecast['Variance']= SWA_UK_Pickup_Forecast['Today\'s Forecast (LOM) Shipment Count']/SWA_UK_Pickup_Forecast['S&OP Forecast Shipment Count']-1
    SWA_UK_Pickup_Forecast= SWA_UK_Pickup_Forecast[['ForecastWeek', 'ForDate', 'ForecastDayOfWeek', 'MerchantId',
                                                'ForecastAccountId', 'StopId', 'ProgramType', 'ServiceType',
                                                'ShipperName', 'WarehouseName', 'NodeId', 'RoutingPrefix',
                                                'Today\'s Forecast (LOM) Shipment Count',
                                                'Today\'s Forecast (LOM) Pallet Count',
                                                'Today\'s Forecast (LOM) Volume (cubic meters)',
                                                'S&OP Forecast Shipment Count',
                                                'LOM vs S&OP shipment count',
                                                'Variance',
                                                'Today\'s Forecast (LOM) SPP',
                                                'Sort Forecast',
                                                'Uncapped LOM', 'Actual LOM']]
        
    return SWA_UK_Pickup_Forecast


###########################################################
##part 2 weekly data swa reformating
###########################################################
def weekly_data_swa_formatting(df):
    weekly_forecast_swa_formatted= df.loc[4:].reset_index()

# drop the first column
    weekly_forecast_swa_formatted = weekly_forecast_swa_formatted.iloc[: , 1:] 
    weekly_forecast_swa_formatted.iloc[0,1] = "SC_DS_individual"

#make the first row of the data
    weekly_forecast_swa_formatted.columns = weekly_forecast_swa_formatted.iloc[0]

# drop the first row
    weekly_forecast_swa_formatted = weekly_forecast_swa_formatted.iloc[1: , :] 

#drop the last row
    weekly_forecast_swa_formatted.drop(weekly_forecast_swa_formatted.tail(1).index,inplace=True)

#colease DS and FC's column name
#weekly_forecast_swa_formatted['SC_DS_individual']=weekly_forecast_swa_formatted['SC_DS_individual'].combine_first(weekly_forecast_swa_formatted['Delivery Stations/Sort Centres'])

#delete rows that are all nan
    weekly_forecast_swa_formatted=weekly_forecast_swa_formatted.dropna(how='all').reset_index()
# drop the first column
    weekly_forecast_swa_formatted = weekly_forecast_swa_formatted.iloc[: , 1:] 

#back fill the column [Delivery Stations/Sort Centres]
    weekly_forecast_swa_formatted['Delivery Stations/Sort Centres'].fillna(method='ffill',inplace = True)

    weekly_forecast_swa_formatted.columns= weekly_forecast_swa_formatted.columns.astype(str)

    return weekly_forecast_swa_formatted

    
       
###########################################################
##part 3 summary pivot from both tables - Node level
###########################################################
def node_level_summary(df1,df2):
    node_level_summary = df1[['NodeId','Today\'s Forecast (LOM) Shipment Count']]
    node_level_summary = node_level_summary.groupby('NodeId',as_index = False)['Today\'s Forecast (LOM) Shipment Count'].sum()

#==========================================
# 3) change the adjustor - which date of the forcast
    node_level_weekly_lookup = df2[['SC_DS_individual','2022-06-08 00:00:00']]
#==========================================

    node_level_summary= pd.merge(node_level_summary,node_level_weekly_lookup,
                                  how = 'left',left_on ='NodeId',right_on ='SC_DS_individual')


    node_level_summary.rename(columns = {'NodeId':'Node',
                                         'Today\'s Forecast (LOM) Shipment Count':'LOM Daily Forecast Shipment Count',
#==========================================
# 4) change the adjustor - which date of the forcast
                                         '2022-06-08 00:00:00':'Revised S&OP Forecast Shipment Count'
                                         }, inplace = True)
#==========================================
                                     
    node_level_summary=node_level_summary[['Node', 'LOM Daily Forecast Shipment Count','Revised S&OP Forecast Shipment Count']]
    node_level_summary['LOM % variance to S&OP'] = (node_level_summary['LOM Daily Forecast Shipment Count']/node_level_summary['Revised S&OP Forecast Shipment Count']-1)*100
    node_level_summary =  pd.merge(node_summary_lookup,node_level_summary,
                                  how = 'left',left_on ='Node',right_on ='Node')

    node_level_summary.loc['Grand Total',:]= node_level_summary.sum(axis=0)#node_level_summary.to_csv(r'C:\Users\cyuanmin\Desktop\Lom and forcast\node_level_summary_test1.csv', index= False)       
    node_level_summary.iloc[-1,0] = 'Grand Total'


##re-calculate the LOM % Variance, as summing is incorrect
    x = str(node_level_summary.iloc[-1,1]).encode('utf-8','ignore')
    y = str(node_level_summary.iloc[-1,2]).encode('utf-8','ignore')
    node_level_summary.iloc[-1,-1] = (float(x)/float(y)-1)*100
    
    for item in list(node_level_summary.columns):
        if item != 'Node':
            node_level_summary[item]= node_level_summary[item].map('{:,.0f}'.format)

    node_level_summary['LOM % variance to S&OP'] = [x+ '%' for x in node_level_summary['LOM % variance to S&OP'] ]
    
    return node_level_summary


###########################################################
##part 4 summary pivot from both tables - SC level
###########################################################
def sc_level_summary(df1,df2):
    sc_level_summary = SWA_UK_Pickup_Forecast[['Sort Forecast','Today\'s Forecast (LOM) Shipment Count']]
    sc_level_summary = sc_level_summary.groupby('Sort Forecast',as_index = False)['Today\'s Forecast (LOM) Shipment Count'].sum()

#==========================================
#5) change the adjustor - which date of the forcast
    sc_level_weekly_lookup = weekly_forecast_swa_formatted[['Delivery Stations/Sort Centres','2022-06-08 00:00:00']]
#==========================================

    sc_level_weekly_lookup['Delivery Stations/Sort Centres']= sc_level_weekly_lookup['Delivery Stations/Sort Centres'].str.strip()
    sc_level_summary= pd.merge(sc_level_summary,sc_level_weekly_lookup,
                                  how = 'left',
                                  left_on ='Sort Forecast',
                                  right_on ='Delivery Stations/Sort Centres')

    sc_level_summary.rename(columns = {'Sort Forecast':'Node',
                                     'Today\'s Forecast (LOM) Shipment Count':'Sum of Today\'s Forecast (LOM) Shipment Count',
#==========================================
#6) change the adjustor - which date of the forcast
                                     '2022-06-08 00:00:00':'Revised S&OP Forecast'
                                     }, inplace = True)
#==========================================
         
    sc_level_summary=sc_level_summary[['Node', 'Sum of Today\'s Forecast (LOM) Shipment Count','Revised S&OP Forecast']]
    sc_level_summary['MNR (3%) buffer'] = sc_level_summary['Sum of Today\'s Forecast (LOM) Shipment Count']*0.03
    sc_level_summary['Capacity Breach (Revised S&OP -LOM -MNR)'] = sc_level_summary['Revised S&OP Forecast'] - sc_level_summary['Sum of Today\'s Forecast (LOM) Shipment Count'] - sc_level_summary['MNR (3%) buffer'] 
    sc_level_summary['LOM % Variance to Revised S&OP'] = (sc_level_summary['Sum of Today\'s Forecast (LOM) Shipment Count'] / sc_level_summary['Revised S&OP Forecast'] - 1)*100

    sc_level_summary =  pd.merge(sc_summary_lookup,sc_level_summary,
                                  how = 'left',
                                  left_on ='Node',
                                  right_on ='Node')


    sc_level_summary.loc['Grand Total',:]= sc_level_summary.sum(axis=0)
    sc_level_summary.iloc[-1,0] = 'Grand Total'

    x = sc_level_summary.iloc[-1,1]
    y = sc_level_summary.iloc[-1,2]
    sc_level_summary.iloc[-1,-1] = (float(x)/float(y)-1) *100

    for item in list(sc_level_summary.columns):
        if item != 'Node':
            sc_level_summary[item]= sc_level_summary[item].map('{:,.0f}'.format)

    sc_level_summary['LOM % Variance to Revised S&OP'] = [x+ '%' for x in sc_level_summary['LOM % Variance to Revised S&OP']]

    return sc_level_summary


###########################################################
##part 6 summary pivot from both tables - SC level and node level
###########################################################
def LOM_summary_pivot_node_level(df1,df2):
    node_level_summary = df1[['NodeId','Today\'s Forecast (LOM) Shipment Count']]
    node_level_summary = node_level_summary.groupby('NodeId',as_index = False)['Today\'s Forecast (LOM) Shipment Count'].sum()
#==========================================
# 3) change the adjustor - which date of the forcast
    node_level_weekly_lookup = df2[['SC_DS_individual','2022-06-08 00:00:00']]
#==========================================
    node_level_weekly_lookup = node_level_weekly_lookup.dropna(axis=0)
    
    node_level_weekly_lookup.loc['Grand Total',:]= node_level_weekly_lookup.sum(axis=0)
    node_level_weekly_lookup.iloc[-1,0] = 'Grand Total'
    return node_level_weekly_lookup


def LOM_summary_pivot_sc_level(df1,df2):
    sc_level_summary = SWA_UK_Pickup_Forecast[['Sort Forecast','Today\'s Forecast (LOM) Shipment Count']]
    sc_level_summary = sc_level_summary.groupby('Sort Forecast',as_index = False)['Today\'s Forecast (LOM) Shipment Count'].sum()

#==========================================
#5) change the adjustor - which date of the forcast
    sc_level_weekly_lookup = weekly_forecast_swa_formatted[['Delivery Stations/Sort Centres','2022-06-08 00:00:00']]
#==========================================

    sc_list =[]
    for item in sc_level_weekly_lookup['Delivery Stations/Sort Centres']:
        if len(item)<=5:
            sc_list.append(item)
            sc_list.append('GRAND TOTAL')       
    sc_level_weekly_lookup= sc_level_weekly_lookup.loc[sc_level_weekly_lookup['Delivery Stations/Sort Centres'].isin(sc_list)]

    return sc_level_weekly_lookup


###########################################################
# Part 7  run functions and Save all results into one Excel
###########################################################

SWA_UK_Pickup_Forecast= transform_lom_report(lom_result) 
weekly_forecast_swa_formatted = weekly_data_swa_formatting(weekly_forecast_swa) 

summary_node_level = node_level_summary(SWA_UK_Pickup_Forecast,weekly_forecast_swa_formatted)
summary_sc_level = sc_level_summary(SWA_UK_Pickup_Forecast,weekly_forecast_swa_formatted)

LOM_summary_pivot_node_level = LOM_summary_pivot_node_level(SWA_UK_Pickup_Forecast,weekly_forecast_swa_formatted)

LOM_summary_pivot_sc_level = LOM_summary_pivot_sc_level(SWA_UK_Pickup_Forecast,weekly_forecast_swa_formatted)


#==========================================
#7) change the adjustor - which date of the folder

with pd.ExcelWriter(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast with Ozge\output file\final_result_2022-06-08.xlsx', engine='xlsxwriter') as writer:    
    # Write each dataframe to a different worksheet.
    summary_sc_level.to_excel(writer, sheet_name='Summary',index = False)
    summary_node_level.to_excel(writer, sheet_name='Sort Vol',index = False)
    SWA_UK_Pickup_Forecast.to_excel(writer, sheet_name='SWA_UK_Pickup_Forecast',index = False)
    
    #LOM_summary_pivot_node_level.to_excel(writer, sheet_name='LOM_summary_pivot_node_level',index = False)
   # LOM_summary_pivot_sc_level.to_excel(writer, sheet_name='LOM_summary_pivot_sc_level',index = False)
    #weekly_forecast_swa_formatted.to_excel(writer, sheet_name='weekly_forecast_swa_formatted',index = False)


#SWA_UK_Pickup_Forecast.to_csv(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast\output file\2022-05-20\SWA_UK_Pickup_Forecast_test1.csv', index= False)      
#weekly_forecast_swa_formatted.to_csv(r'C:\Users\cyuanmin\Documents\Amazon Shipping\Lom and forcast\output file\2022-05-20\weekly_forecast_swa_formatted.csv', index= False)   
#sc_level_summary.to_csv(r'C:\Users\cyuanmin\Desktop\Lom and forcast\input file\2022-05-20\sc_level_summary_test1.csv', index= False)       
    