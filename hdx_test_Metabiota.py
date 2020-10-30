#Useful resources for work
#https://github.com/OCHA-DAP/hdx-python-api
#https://github.com/OCHA-DAP/hdx-python-api/blob/master/src/hdx/hdx_configuration.py
#https://rdrr.io/github/dickoa/rhdx/man/Dataset.html

# API No
# Spatiotemporal data for 2019-Novel Coronavirus Covid-19 Cases and deaths
# https://data.humdata.org/dataset/2019-novel-coronavirus-cases
# Update every day

from hdx.hdx_configuration import Configuration
from hdx.data.dataset import Dataset
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import pandas as pd
import pandas.errors
import datetime
import os
import io

Configuration.create(hdx_site='prod', user_agent='A_Quick_Example', hdx_read_only=True)

# Connect to sources
def connect(source, name):
    try:
        urlopen(source)
    except HTTPError as e:
        print(name, ' - the server couldn\'t fulfill the request')
        print('Error code: ', e.code)
    except URLError as e:
        print(name, ' - we failed to reach a server')
        print('Reason: ', e.reason)
    else:
        print(name, ' - source is working')

# Authentication

# Receiving data
def receiving(source, name):
    dataset = Dataset.read_from_hdx(source)
    last_date = dataset.get_dataset_end_date_as_datetime()
    print("Last update - ", last_date, "\nContent in dataset:")

# Data integrity check
writepath='/home/user/Data_cash/hdx_test2/check_info.txt'

def check(direct):
    mode = 'a' if os.path.exists(writepath) else 'w'
    os.chdir(direct)
    filenames = os.listdir(direct)
    for filename in filenames:
        with open(writepath, mode, encoding="utf-8") as f:
            try:
                df = pd.read_csv(filename)
            except pd.errors.ParserError:
                print(filename, ' - file is parser error')
                f.writelines("\n" + datetime.datetime.now().ctime() + "\n" + filename + " - file is parser error" + "\n")
            except pd.errors.EmptyDataError:
                print(filename, ' - file is empty')
                f.writelines("\n" + datetime.datetime.now().ctime() + "\n" + filename + " - file is empty" + "\n")
            else:
                buffer = io.StringIO()
                df.info(verbose=False, buf=buffer)
                s = buffer.getvalue()
                print(filename, ' - file check passed')
                f.writelines("\n" + datetime.datetime.now().ctime() + "\n" + filename + " - file is normal" + "\n" + s)

# Special purpose

# Save to datacash
Metabiota='/home/user/Data_cash/hdx_test2'

def save(source, direct):
    os.makedirs(direct)
    dataset = Dataset.read_from_hdx(source)
    resources = dataset.get_resources()

    for res in resources:
        url, path = res.download(folder=direct)
        print('Resource URL %s downloaded to %s' % (url, path))

query_dataset = 'https://data.humdata.org/dataset/'
query_dataset_url = '2019-novel-coronavirus-cases'
query_dataset_name = 'Spatiotemporal data for 2019-Novel Coronavirus Covid-19 Cases and deaths'

connect(source=query_dataset+query_dataset_url, name=query_dataset_name)
receiving(source=query_dataset_url, name=query_dataset_name)
save(source=query_dataset_url, name=query_dataset_name, direct=Metabiota)
check(direct=Metabiota)





