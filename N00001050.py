#https://github.com/vespa-engine/cord-19/blob/master/cord-19-queries.md
#https://question-it.com/questions/383748/vlozhennyj-json-v-csv-obschij-podhod
#https://json-csv.com/
#http://datalytics.ru/all/kak-v-pandas-razbit-kolonku-na-neskolko-kolonok/

# API No 00001050
# CORD-19
# https://github.com/vespa-engine/cord-19/blob/master/cord-19-queries.md
# Update every day

import requests
import pandas as pd
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import io
import os
import datetime

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

# Request parameters for Russia
def receiving(query, hits):
  search_request_any = {
    'yql': 'select id,title, abstract, body_text, timestamp, doi from sources * where userQuery();',
    'hits': hits,
    'summary': 'short',
    'timeout': '1.0s',
    'query': query,
    'type': 'all',
    'ranking': 'default'
  }
  endpoint = 'https://api.cord19.vespa.ai/search/'
  response = requests.post(endpoint, json=search_request_any)
  result = response.json()
  df = pd.DataFrame(result)
  df = df['root']['children']
  df = pd.io.json.json_normalize(df)
  print(df)
  return df

# Data integrity check
writepath='/home/user/Data_cash/N00001050/check_info.txt'
#writepath='C:/Users/tred1/Desktop/CORD-19/log10.txt'

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
def save(name, direct, df):
    path = direct % name
    #os.mkdir(path)
    df.to_csv(path, encoding='utf-8')
    print("Download complete")

director = '/home/user/Data_cash/N00001050'
os.makedirs(director)
naming = '/%s.csv'
directory = director + naming
#directory = 'C:/Users/tred1/Desktop/CORD-19/%s.csv'

key_phrase = "Russia"
number_of_results = 1000

connect(source='https://cord19.vespa.ai/', name='CORD-19 Search')
df_rez = receiving(query=key_phrase, hits=number_of_results)
save(name=key_phrase, direct=directory, df=df_rez)
check(direct=director)



