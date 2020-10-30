# API No N00001010
# Novel Corona Virus 2019 Dataset
# https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset
# Last update a month ago

from kaggle.api.kaggle_api_extended import KaggleApi
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import pandas as pd
import pandas.errors
import datetime
import os
import io

api = KaggleApi()

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
def authentication():
    api.authenticate()

# Receiving data

# Data integrity check
writepath='/home/user/Data_cash/N0001010/check_info.txt'

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
    print("Download ", name, " from Kaggle")
    api.dataset_download_files(dataset=source, path=direct, force=False, quiet=True, unzip=True)
    print("Download complete")

directory = '/home/user/Data_cash/N0001010'

query_dataset = 'https://www.kaggle.com/'
query_dataset_url = 'sudalairajkumar/novel-corona-virus-2019-dataset'
query_dataset_name = 'Novel Corona Virus 2019 Dataset'

connect(source=query_dataset + query_dataset_url, name=query_dataset_name)
authentication()
save(source=query_dataset_url, name=query_dataset_name, direct=directory)
check(direct=directory)

