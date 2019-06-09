import numpy as np
import pandas as pd
import datetime as dt
from functools import reduce
import pygrib
import os

import gns_scrapper


######################################################################################
######################################################################################

class Region():

    def __init__(self, name, longitude_range, latitude_range):
        self.name = name
        self.longitude_range = longitude_range
        self.latitude_range = latitude_range

######################################################################################
######################################################################################



# get weather value for the specified area
# lon_range can only be tuple of strings if coordinate system not 360
#coordinates_system of longitude one of the following (lat always between -90 & 90):
# 1) 360 : no action
# 2) EW  : format (##E, ##W) where ## is positive e.g. (47.50E, 110.25W) -> (47.50, 249.75)
# 3) E   : format (##E, ##E) where ## can be negative e.g. (47.5E, -50E) -> (47.50. 310)
#
# call as following for Chicago: a = get_weather('Maximum temperature', lon_range = ('88.3W', '87.3W'), lat_range = (42.3, 41.3), coordinate_system='EW')
def get_weather(weather_var, lon_range, lat_range, model_date, model_hour, files, coordinate_system='EW'):

    '''
    Only used in get_data
    '''

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
    print('The shape of the selected area is: {0} x {1}'.format(int(lon_width/resol), int(lat_width/resol)))
    print('Number of points involved in the parameter estimation: {0}'. format(int(lon_width/resol) * int(lat_width/resol)))

    model_datetime = dt.datetime.strptime(model_date + model_hour, '%Y%m%d%H')
    weather_var_values = dict()

    for f in files:
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


    return weather_var_values

#variables is a list of the variable names
#case and spelling sensitive, have to be identical!!
#mode = latest OR custom for user input OR prespecified for already passed params in model_date, model_time
def get_data(location, variables, model_date=None, model_hour=None, func_to_apply='mean'):

    '''
    ONLY USED WHEN weather_monitor.py IS RUN

    ONLY TIME get_weather IS USED
    ONLY PLACE check_data_syncing IS USED OTHER THAN THE TWO LINES IN weather_monitor.py
        sync_latest = gns.check_data_syncing(latest_model[:-2], latest_model[-2:], True)
        sync_previous = gns.check_data_syncing(previous_model[:-2], previous_model[-2:], True)
    '''

    gns_dir = os.path.join(os.getcwd() + '/' + 'GNS')

    os.chdir(gns_dir)

    # If model_date and model_hour are passed in as none, this block of code will get those by using pick_gfs_model(), and try to change into the directory for that date, and hour
    if model_date is None or model_hour is None:
        print("'model_date and model_time not passed'")
        print("Using the latest model")
        mode='latest'
        model_date, model_hour = pick_gfs_model(mode)
    try:
        os.chdir(model_date)
        os.chdir(model_hour)
    except:
        print('Model date: {}'.format(model_date))
        print('Model hour: {}'.format(model_hour))
        raise ValueError('The folder hierarchy is not correct !!')
        return

    files = os.listdir()
    print('Getting data for: ', location.name)

    #check if it is only one variable, and if it is (just a string) this will turn it into a list
    if type(variables)==str:
        variables = [variables]

    forecast_data = dict()
    for v in variables:
        #e.g. forecast_data_tavg = get_weather('2 metre temperature', location.longitude_range, location.latitude_range, model_date, model_hour,files)
        temp = get_weather(v, location.longitude_range, location.latitude_range, model_date, model_hour, files)
        df_temp = pd.Series(temp, name=location.name + ' ' + v).to_frame()
        forecast_data[v] = df_temp.groupby([df_temp.index.date]).agg(func_to_apply)

    # Changing directory back to root
    os.chdir('..')
    os.chdir('..')
    os.chdir('..')

    return forecast_data


def pick_gfs_model(mode='auto'):

    '''
    Just returns a tuple of (date,time)
    Example: ('20190101', '00')
    '''

    if mode=='latest':
        today_date = dt.datetime.now().strftime('%Y%m%d')
        today_time_now = dt.datetime.now().strftime('%H')
        model_hours = ['00', '06', '12', '18']
        m = int(int(today_time_now)/6) + 1
        m_hour = model_hours[m-1]
    elif mode=='custom':
        today_date = str(int(input('>> Give model date in the format [YYYYMMDD]: ')))
        m_hour = str(int(input('>> Give model hour: ')))

    return today_date, m_hour

def check_data_syncing(model_date, model_time, sync=True):
    root = os.path.join(os.getcwd() + '/' + 'GNS')

    # Checking if files have been downloaded for the date and time inputted. boolean.
    date_exists = os.path.exists(root + '/' + model_date)
    time_exists = os.path.exists(root + '/' + model_date + '/' + model_time)

    # With gec / gep files f384 still works
    if date_exists and time_exists:

        return True

    else:

        if sync:

            # sync will be passed in as true in weather_monitor.py. This is to make sure that the files are downloaded, because if the function ends up here, that means
            # either the date or time did not exist as a valid path in thr check above
            gns_scrapper.run(model_date, model_time)
            return check_data_syncing(model_date, model_time)

        else:
            return False


######################################################################################
######################################################################################
######################################################################################

if __name__ == '__main__':

    # get_data(location, variables, model_date=None, model_hour=None, func_to_apply='mean')
    # reg = Region('Texas', ('105.25W', '93.7W'), (26.25, 34.5))
    # chicago = Region('Chicago', ('88.3W', '87.3W'), (42.3, 41.3))
    # data = get_data(reg, '2 metre temperature')
    # chic = get_data(chicago, '2 metre temperature')
    # print(chic)
    ####################################
    #Regions more detailed

    #east
    e1 = Region('e1', ('85W', '70W'), (40, 47))
    e2 = Region('e2', ('75W', '82.5W'), (33, 40))
    e3 = Region('e3', ('80W', '85.5W'), (25, 35))
    east = [e1, e2, e3]

    #midwest
    mw1 = Region('mw1', ('97W', '82.5W'), (35, 49))
    midwest = [mw1]

    #south central
    s1 = Region('s1', ('103W', '91W'), (37, 27))
    s2 = Region('s2', ('91W', '85W'), (35, 29))
    south_central = [s1, s2]

    #mountain
    mt1 = Region('mt1', ('117W', '96W'), (49, 40))
    mt2 = Region('mt2', ('102W', '117.5W'), (37, 40))
    mt3 = Region('mt3', ('103W', '115W'), (37, 32))
    mountain = [mt1, mt2, mt3]

    #pacific
    p1 = Region('p1', ('117W', '124W'), (49, 42))
    p2 = Region('p2', ('124W', '120W'), (37.5, 42))
    p3 = Region('p3', ('114W', '121W'), (32.5, 37))
    pacific = [p1, p2, p3]


    regions = {'East':east,
              'Midwest':midwest,
              'South Central':south_central,
              'Mountain':mountain,
              'Pacific':pacific}


    var = '2 metre temperature'

    dfs_latest = []
    for reg in regions.keys():
        dfs_temp = []
        for subregion in regions[reg]:

            # ONLY TIME GET_DATA IS USED
            temp = get_data(subregion, var)
            dfs_temp.append(temp[var])
        d = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), dfs_temp).mean(axis=1).to_frame()
        d.columns = [reg]
        dfs_latest.append(d)

    regions_df = pd.concat(dfs_latest, axis=1)

    day_time_tup = pick_gfs_model('latest')
    model_date, model_hour = day_time_tup[0], day_time_tup[1]

    gns_csvs_dir = os.path.join(os.getcwd() + '/' + 'GNS_CSVs')

    if os.path.exists(gns_csvs_dir):
        os.chdir(gns_csvs_dir)
    else:
        os.mkdir(gns_csvs_dir)
        os.chdir(gns_csvs_dir)

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



    regions_df.to_csv('model_frame')

    os.chdir('..')
    os.chdir('..')
    os.chdir('..')
