import urllib3
import datetime as dt
from ftplib import FTP
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import time
import re
import os

import urllib3
import datetime as dt
from ftplib import FTP
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import time
import re
import os

def download_files(mode='latest', model_date=None, model_hour=None):
    root_url_gfs = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/'

    if mode=='latest':
        print('Mode: Latest..')
        today_date = dt.datetime.now().strftime('%Y%m%d')
        today_time_now = dt.datetime.now().strftime('%H')

        model_hours = ['00', '06', '12', '18']
        m = int(int(today_time_now)/6) + 1
        m_hour = model_hours[m-1]
    elif mode=='user':
        print('Mode: User specified model datetime/ Not the latest..')
        today_date = str(int(input('>> Give model date in the format [YYYYMMDD]: ')))
        m_hour = str(int(input('>> Give model hour: ')))
    elif mode=='specific_model':
        print('Mode: Already passed model date & time')
        today_date = model_date
        m_hour = model_hour

    model_name = today_date + '/' + m_hour
    model_files_url = root_url_gfs + 'gefs.' + model_name + '/' + 'pgrb2' + '/'

    #downloading part
    http = urllib3.PoolManager()
    r = http.request('GET', model_files_url)
    if r.status != 200:
        raise ValueError("The url the model files can't be loaded")
    else:
        soup = bs(r.data, 'html.parser')

    # gec12.t12z.pgrb2f102
    mask = re.compile('ge\w\d{2}\.t\d{2}z\.pgrb2f\d{2,3}$')
    # mask = re.compile('gec00\.t\d{2}z\.pgrb2f\d{3}$')

    files = []
    for l in soup.find_all('a'):

        if bool(re.match(mask, l['href'])) and len(l['href'])>=19 and len(l['href'])<21: files.append(l['href'])

    gns_dir = os.path.join(os.getcwd() + '/' + 'GNS')

    if os.path.exists(gns_dir):
        os.chdir(gns_dir)
    else:
        os.mkdir(gns_dir)
        os.chdir(gns_dir)

    #check if date folder exists
    if os.path.exists(today_date):
        pass
    else:
        os.mkdir(today_date)
    os.chdir(today_date)

    #check if time exists
    if os.path.exists(m_hour):
        pass
    else:
        os.mkdir(m_hour)
    os.chdir(m_hour)

    print('Starting downloading process...')
    file_root_url = 'http://nomads.ncep.noaa.gov/cgi-bin/filter_gens.pl?file={0}&lev_2_m_above_ground=on&var_TMAX=on&var_TMIN=on&var_TMP=on&subregion=&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgefs.{1}%2F{2}%2Fpgrb2'

    for f in files:

        try:

            f_url = file_root_url.format(f, today_date, m_hour)
            filename = f
            print(filename)
            content = requests.get(f_url, timeout = 20).content
            with open(filename, 'wb') as gribfile:
                gribfile.write(content)
            time.sleep(0.1)

        except:

            print("########################################", f)

    os.chdir('..')
    os.chdir('..')
    os.chdir('..')
    
    print('Download complete!')

    return


def run(model_date=None, model_time=None):

    if model_date is None and model_time is None:
        mode = 'latest'
    else:
        mode = 'specific_model'

    while mode!='latest' and mode!='user' and mode!='specific_model' :
        mode = input('>> Select download mode [latest/ user]: ')


    download_files(mode, model_date, model_time)
    return

if __name__=='__main__':

    run()
