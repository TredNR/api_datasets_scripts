#Useful resources for work
#https://github.com/OCHA-DAP/hdx-python-api
#https://rdrr.io/github/dickoa/rhdx/man/Dataset.html
#https://github.com/CSSEGISandData/COVID-19

# API No
# Novel Coronavirus (COVID-19) Cases Data
# https://data.humdata.org/dataset/novel-coronavirus-2019-ncov-cases
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
def receiving(source):
    dataset = Dataset.read_from_hdx(source)
    last_date = dataset.get_dataset_end_date_as_datetime()
    print("Last update - ", last_date)

# Data integrity check
writepath='/home/user/Data_cash/hdx_test/check_info.txt'

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
def save(source, name, direct):
    os.makedirs(direct)
    dataset = Dataset.read_from_hdx(source)
    resources = dataset.get_resources()

    for res in resources:
        if name == 'Novel Coronavirus (COVID-19) Cases Data':
            direct == Johns_Hopkins
        elif name == 'Coronavirus (COVID-19) Cases and Deaths':
            direct == WHO
        url, path = res.download(folder=direct)
        print('Resource URL %s downloaded to %s' % (url, path))

Johns_Hopkins = '/home/user/Data_cash/hdx_test/Johns_Hopkins_university'
WHO = '/home/user/Data_cash/hdx_test/World_Health_Organization'

dataset_list = [['novel-coronavirus-2019-ncov-cases', 'Novel Coronavirus (COVID-19) Cases Data', Johns_Hopkins], ['coronavirus-covid-19-cases-and-deaths', 'Coronavirus (COVID-19) Cases and Deaths', WHO]]
query_dataset = 'https://data.humdata.org/dataset/'

for data in dataset_list:
    connect(source=query_dataset+data[0], name=data[1])
    receiving(source=data[0])
    save(source=data[0], name=data[1], direct=data[2])
    check(direct=data[2])





