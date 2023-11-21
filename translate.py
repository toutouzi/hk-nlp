# 用来补充nllb的训练集

import requests, os

def translate(text, src_lang='auto', to_lang='en'):
    '''
    Google翻译
    '''
    googleapis_url = 'https://translate.googleapis.com/translate_a/single'
    params = {
        'client': 'gtx',
        'sl': src_lang,
        'tl': to_lang,
        'dt': 't',
        'q': text
    }

    try:
        data = requests.get(googleapis_url, params=params)
        if data.status_code == 200:
            data = data.json()
            translations = [s[0] for s in data[0] if s[0]]
            return ' '.join(translations)
        else:
            return f'Error: {data.status_code}'
    except requests.RequestException as e:
        return f'Request Exception: {e}'
    except ValueError as e:
        return f'ValueError: {e}'


import numpy as np
import pandas as pd
from datetime import datetime
to_be_tran = pd.read_parquet('../all_job_descriptions_job_title_translation/jobtitle_count_summary/wmj_jobtitle_translated_51946844.parquet')
save_path = './'
df_list = np.array_split(to_be_tran, 47)
for i, df in enumerate(df_list):
  save_name = os.path.join(save_path, f'skill_only_26others{i:0>2}.parquet')
  if os.path.isfile(save_name):
    continue
  print(save_name)
  print(f'start translate {i} ', datetime.now())
  jobtitlelist = df['skill_jobtitle'].tolist()
  all_trans = []
  for id, skill in enumerate(jobtitlelist):
     tran = translate(skill)
     if id % 1000 == 0: 
       print(tran)
     all_trans.append(tran)
  print('saved', datetime.now())
  df['google_translated'] = all_trans
  df.to_parquet(save_name)
