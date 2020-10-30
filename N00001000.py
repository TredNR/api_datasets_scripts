# API No N00001000
# The COVID Tracking Project by Atlantic Monthly Group
# https://covidtracking.com/data/api - but more like a dataset
# Update database in 4 pm ET
# https://covidtrackingproject.statuspage.io/ - status page

import pandas as pd
import pandas.errors
import datetime
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from tqdm import tqdm
import os
import io

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

# Data integrity check
writepath = "/home/user/Data_cash/N00001000/check_info.txt"

def check(source, name):
    mode = 'a' if os.path.exists(writepath) else 'w'
    with open(writepath, mode, encoding="utf-8") as f:
        try:
            df = pd.read_csv(source)
        except pd.errors.ParserError:
            print(name, ' - file is parser error')
            f.writelines("\n" + datetime.datetime.now().ctime() + "\n" + name + " - file is parser error" +  "\n" + source)
        except pd.errors.EmptyDataError:
            print(name, ' - file is empty')
            f.writelines("\n" + datetime.datetime.now().ctime() + "\n" + name + " - file is empty" + "\n" + source)
        else:
            buffer = io.StringIO()
            df.info(verbose=False, buf=buffer)
            s = buffer.getvalue()
            print(name, ' - file check passed')
            f.writelines("\n" + datetime.datetime.now().ctime() + "\n" + name + " - file is normal" + "\n" + s)

# Special purpose

# Save to datacash
def save(source, name=None, name_state_hist=None, name_state_now=None):
    tqdm.pandas(desc=name_state_hist or name_state_now or name)

    if name_state_hist is not None:
        direct = directory_hist % name_state_hist
    elif name_state_now is not None:
        direct = directory_now % name_state_now
    else:
        direct = directory % name

    mysave = pd.read_csv(source)
    mysave = mysave.progress_apply(lambda x: x.replace('*', ''))
    mysave.to_csv(direct)
    return mysave

directory_hist = '/home/user/Data_cash/N00001000/state_hist/%s.csv'
directory_now = '/home/user/Data_cash/N00001000/state_now/%s.csv'
directory = '/home/user/Data_cash/N00001000/%s.csv'

states_list = [['ak', 'Alaska'], ['al', 'Alabama'], ['ar', 'Arkansas'], ['as', 'American Samoa'], ['az', 'Arizona'], ['ca', 'California'],
                   ['co', 'Colorado'], ['ct', 'Connecticut'], ['dc', 'District of Columbia'], ['de', 'Delaware'], ['fl', 'Florida'], ['ga', 'Georgia'],
                   ['gu', 'Guam'], ['hi', 'Hawaii'], ['ia', 'Iowa'], ['id', 'Idaho'], ['il', 'Illinois'], ['in', 'Indiana'],
                   ['ks', 'Kansas'], ['ky', 'Kentucky'], ['la', 'Louisiana'], ['ma', 'Massachusetts'], ['md', 'Maryland'], ['me', 'Maine'],
                   ['mi', 'Michigan'], ['mn', 'Minnesota'], ['mo', 'Missouri'], ['mp', 'Northern Mariana Islands'], ['ms', 'Mississippi'], ['mt', 'Montana'],
                   ['nc', 'North Carolina'], ['nd', 'North Dakota'], ['ne', 'Nebraska'], ['nh', 'New Hampshire'], ['nj', 'New Jersey'], ['nm', 'New Mexico'],
                   ['nv', 'Nevada'], ['ny', 'New York'], ['oh', 'Ohio'], ['ok', 'Oklahoma'], ['or', 'Oregon'], ['pa', 'Pennsylvania'],
                   ['pr', 'Puerto Rico'], ['ri', 'Rhode Island'], ['sc', 'South Carolina'], ['sd', 'South Dakota'], ['tn', 'Tennessee'], ['tx', 'Texas'],
                   ['ut', 'Utah'], ['va', 'Virginia'], ['vi', 'Virgin Islands'], ['vt', 'Vermont'], ['wa', 'Washington'], ['wi', 'Wisconsin'],
                   ['wv', 'West Virginia'], ['wy', 'Wyoming']]

query_states = 'https://api.covidtracking.com/v1/states/'
query_current = '/current.csv'
query_daily = '/daily.csv'
query_info = '/info.csv'
query_us = 'https://api.covidtracking.com/v1/us/'

# Connection to the site
connect(source='https://api.covidtracking.com', name='api.covidtracking.com')
connect(source=query_us + query_current, name='us_current')
connect(source=query_us + query_daily, name='us_daily')

# Checking CSV integrity
check(source=query_us + query_current, name='us_current')
check(source=query_us + query_daily, name='us_daily')

# General tables that include US and state totals
    #Save CSV file
save(source=query_us + query_current, name='us_current')
save(source=query_us + query_daily, name='us_daily')
save(source=query_states + query_current, name='all_states_current')
save(source=query_states + query_daily, name='all_states_daily')
save(source=query_states + query_info, name='state_metadata')

for state in states_list:
    # The most recent COVID data for a single state (now)
    save(source=query_states + state[0] + query_current, name_state_now=state[1] + "_now")

    # All COVID data for a single state (historic)
    save(source=query_states + state[0] + query_daily, name_state_hist=state[1] + "_hist")