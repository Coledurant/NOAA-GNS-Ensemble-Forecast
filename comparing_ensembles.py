import gns_scrapper
import gns_weather
from send_email import send_email

import os
import pandas as pd
import numpy as np
import datetime as dt
from functools import reduce
import pygrib
import os
import urllib3
import datetime as dt
from ftplib import FTP
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import time
import re
from sys import platform as sys_pf
if sys_pf == 'darwin':
    import matplotlib
    matplotlib.use("TkAgg")

    import matplotlib.pyplot as plt

##############################################################################################################################
##############################################################################################################################
##############################################################################################################################

root = os.getcwd()
gns_dir = os.path.join(root + '/' + 'GNS')
gns_plots_dir = os.path.join(root + '/' + 'GNS_PLOTS')


day_time_tup = gns_weather.pick_gfs_model('latest')
model_date, model_hour = day_time_tup[0], day_time_tup[1]

gns_weather.check_data_syncing(model_date, model_hour, sync=True)


#east
e1 = gns_weather.Region('e1', ('85W', '70W'), (40, 47))
e2 = gns_weather.Region('e2', ('75W', '82.5W'), (33, 40))
e3 = gns_weather.Region('e3', ('80W', '85.5W'), (25, 35))
east = [e1, e2, e3]

#midwest
mw1 = gns_weather.Region('mw1', ('97W', '82.5W'), (35, 49))
midwest = [mw1]

#south central
s1 = gns_weather.Region('s1', ('103W', '91W'), (37, 27))
s2 = gns_weather.Region('s2', ('91W', '85W'), (35, 29))
south_central = [s1, s2]

#mountain
mt1 = gns_weather.Region('mt1', ('117W', '96W'), (49, 40))
mt2 = gns_weather.Region('mt2', ('102W', '117.5W'), (37, 40))
mt3 = gns_weather.Region('mt3', ('103W', '115W'), (37, 32))
mountain = [mt1, mt2, mt3]

#pacific
p1 = gns_weather.Region('p1', ('117W', '124W'), (49, 42))
p2 = gns_weather.Region('p2', ('124W', '120W'), (37.5, 42))
p3 = gns_weather.Region('p3', ('114W', '121W'), (32.5, 37))
pacific = [p1, p2, p3]


regions = {'East':east,
          'Midwest':midwest,
          'South Central':south_central,
          'Mountain':mountain,
          'Pacific':pacific}


var = '2 metre temperature'

ensemble_names = ['control','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20']

##############################################################################################################################

os.chdir(gns_dir)
os.chdir(model_date)
os.chdir(model_hour)

files = os.listdir()

# Ensemble 1: [23,43,36, etc...]
ensemble_forecasts_dict = dict()

# 1: gep01 etc...
file_per_forecast_dict = dict()

for file in files:

    # Find which files belong to which ensembles

    if 'gec' in file:
        # File is a control forecast
        if 'control' not in list(file_per_forecast_dict.keys()):
            file_per_forecast_dict['control'] = [file]
        else:
            file_per_forecast_dict.get('control').append(file)
    else:
        # File is one of the ensembles

        # gep{12} -> number ensemble is the number that comes after 'gep'
        # Keeping the number ensemble as a string for dataframe column names later
        number_ensemble = file[3:5]
        if number_ensemble not in list(file_per_forecast_dict.keys()):
            # Adding a new list for the ensemble since there was not one before
            file_per_forecast_dict[number_ensemble] = [file]
        else:
            # Appending the file since a list already exists for this ensemble
            file_per_forecast_dict.get(number_ensemble).append(file)

os.chdir(root)

##############################################################################################################################

def get_ensemble_weather(subregion, weather_var, file_forecast_dict, model_date, model_hour, coordinate_system = 'EW'):

    lon_range = subregion.longitude_range
    lat_range = subregion.latitude_range

    #fix longitude to match NOAA
    if coordinate_system=='360':
        pass
    elif coordinate_system=='EW':
        lr = []
        for l in list(lon_range):
            if 'W' in l: l = 360 - float(l[:-1])
            elif 'E' in l: l = float(l[:-1])
            lr.append(l)
        lon_range = tuple(lr)
    elif coordinate_system=='E':
        lr = []
        for l in list(lon_range):
            l = float(l[:-1])
            lr.append(l)
        lon_range = tuple(lr)

    resol = 1
    lat_width = max(lat_range) - min(lat_range)
    lon_width = max(lon_range) - min(lon_range)

    model_datetime = dt.datetime.strptime(model_date + model_hour, '%Y%m%d%H')

    every_ensemble_frame = []

    for ensemble_name, ensemble_file_list in file_forecast_dict.items():

        weather_var_values = dict()

        os.chdir(gns_dir)
        os.chdir(model_date)
        os.chdir(model_hour)

        for f in ensemble_file_list:
            grbs = pygrib.open(f)
            for grb in grbs:
                try:
                    grb = grbs.select(name=weather_var)[0]
                    forecasted_datetime = model_datetime + dt.timedelta(hours = grb['endStep'])

                    data, lats, lons = grb.data(lat1=min(lat_range),lat2=max(lat_range),lon1=min(lon_range),lon2=max(lon_range))
                    value = data.mean()

                    if 'temperature' in weather_var.lower():
                        #convert to F
                        value = value * 9.0/5 -459.67
                    weather_var_values[forecasted_datetime] = value

                except:
                    print('Variable: {0} not found in file {1}'.format(weather_var, f))

        df_temp = pd.Series(weather_var_values, name = ensemble_name).to_frame()

        # Average value per date sorted and put into a dataframe for the subregion
        grouped_and_avg_temp_df_for_ensemble = df_temp.groupby([df_temp.index.date]).agg('mean')

        every_ensemble_frame.append(grouped_and_avg_temp_df_for_ensemble)

    os.chdir(root)

    return pd.concat(every_ensemble_frame, axis=1)

##############################################################################################################################


for region in regions:

    print(region)

    # List of dataframes for each ensemble per subregion
    subregions_forecasts_list = []
    for subregion in regions[region]:

        # This is getting a dataframe for the subregion with columns as ensemble numbers and an index as datetime values
        subregion_ensemble_frame = get_ensemble_weather(subregion, var, file_per_forecast_dict, model_date, model_hour)
        subregions_forecasts_list.append(subregion_ensemble_frame)

    # Averaged dataframe from each subregions dataframe
    averaged_region_frame = pd.concat(subregions_forecasts_list).groupby(level=0).mean()

    os.chdir(root)

    if os.path.exists(gns_plots_dir):
        os.chdir(gns_plots_dir)
    else:
        os.mkdir(gns_plots_dir)
        os.chdir(gns_plots_dir)

    if os.path.exists(model_date):
        os.chdir(model_date)
    else:
        os.mkdir(model_date)
        os.chdir(model_date)

    if os.path.exists(model_hour):
        os.chdir(model_hour)
    else:
        os.mkdir(model_hour)
        os.chdir(model_hour)


    plt.style.use('seaborn-white')
    control_column = averaged_region_frame['control']
    averaged_region_frame.drop('control', axis=1, inplace=True)
    ax = averaged_region_frame.plot(figsize=(20,10), linewidth=2.5, fontsize=12, grid=True, color='grey')
    control_column.plot(color='r', legend=True, grid=True)
    plt.xlabel('Date', fontsize=15)
    plt.ylabel('Temperature (F)', fontsize=15)
    plt.title('{0} region ensemble forecast for the average 2 meter above ground temperature'.format(region), fontsize=15)
    ax.legend(loc='lower left', bbox_to_anchor= (1.01, 0.0), ncol=1,
            borderaxespad=0, frameon=False)
    plt.savefig('{0}_region_ensemble_forecast_for_the_average_2_meter_above_ground_temperature'.format(region))


################################################ Email Part ################################################


subject = 'GNS weather monitor - model: {0}hr'.format(model_hour)

recipients = ['cole.durant@gmail.com', 'cdurant@northerntrace.com', 'oliviakuns@gmail.com', 'ndurant75@gmail.com','mdurant27@gmail.com']

message_field = 'Latest model: {0}'.format(model_hour) + '\n' + '\n' \
                    + 'Attatched is: ' + '\n' \
                    + '1.) Current ensemble forecasts for the EIA regions (in F):'

# df_frames = ['model_frame.html']
img_list = ['{0}_region_ensemble_forecast_for_the_average_2_meter_above_ground_temperature.png'.format(region) for region in regions]

send_email('gnsalertsender@gmail.com', 'gnsalert',recipients, subject, message_field, images_list=img_list)

os.chdir(root)
