import collections

from ortools.sat.python import cp_model

from drawing import draw

import pandas as pd
import numpy as np
from model import *
from commonstr import *
import openpyxl



task_type = collections.namedtuple('task', 'client_idx priority_idx duration ready_time order')

job_data = {}  # key by key_idx, inside need to store tasks in order which may have same tasks at different stage


paras = {
  'unfinished': None, #unfinished job from befor b4
  max_job_str: 420,
  'days': 5,
  'start': 8,  # start time for non embedding stage
  'end': 23,  # end time for non embedding stage,  8pm - 5am
  'start_emdbedding': [12 * seconds_per_hour, 20 * seconds_per_hour],
  # start time for category 0 and 1 at stage 2, 12pm, and 6 pm
  'duration_2': [2 * seconds_per_hour, 9 * seconds_per_hour],  # duration for category 0 and 1 at embedding in seconds

  'max_serach_time_sec': 600,
  'capacity':{0:3, 1: 12,2: 1000, 3: 8, 4:4},
  job_weights_str: {},
  'result': [],
  'full' : False,
  'day_jobs':{}
}



def examine_case(case_idx, day_index = 0):
  print (paras[idx_to_name_key_str][case_idx])
  tasks =  job_data[case_idx]
  for idx, task in enumerate(tasks):

    client = paras[idx_to_name_client_str][task.client_idx]
    priority = paras[idx_to_name_priority_str][task.priority_idx]
    duration = task.duration
    ready = format_time(task.ready_time)
    print(f'client {client} priority {priority} duration {duration} ready {ready} ready {task.ready_time}')
      
    pd_ready_time = pd.to_datetime(ready)
    pd_day_end = pd.to_datetime(day_endings[day_index])
    if (idx ==0):
     assert(pd_ready_time < pd_day_end)


def load_real_data():

  df = pd.read_csv("3-day.csv")
  df['start_timestamp'] = pd.to_datetime(df['start_timestamp'], format='%Y-%m-%d %H:%M:%S')
  df['end_timestamp'] = pd.to_datetime(df['end_timestamp'], format='%Y-%m-%d %H:%M:%S')
  df['work_ready_timestamp'] = pd.to_datetime(df['work_ready_timestamp'], format='%Y-%m-%d %H:%M:%S')
    # https://www.kite.com/python/answers/how-to-find-the-number-of-seconds-between-two-datetime-objects-in-python
  print(df.work_ready_timestamp.head(3))

  print(df.head(3))
  past_date = pd.to_datetime('17/05/2021 00:00')

  df['duration'] = df['end_timestamp'] - df["start_timestamp"]
  df['duration_sec'] = df['duration'] / np.timedelta64(1,'s')

  df['ready_time_sec'] = df['work_ready_timestamp'].apply(lambda x: (x - past_date).total_seconds())

  f =  df[df.case_key == 'ff65e4c1533550f73c6986d629fad87cbb9b75c1aab469bc8b174f15862ee65b']
  print('f is ', f['work_ready_timestamp'].values, f['ready_time_sec'].values)

  # remove jobs that has more than 2 embeddings, first need to count
  # count how many embeeddings for each key
  # first find all rows whose client is embeddings

  f = df[df.client == 'Embedding']
  f = f.groupby(by='case_key').client.value_counts()
  count_dict = {}
  # create a dict of case_key, embedding value count
  for i, v in f.iteritems():
    #print('index: ', i[0], 'value: ', v)
    count_dict[i[0]] = v

  df['embedding_count'] = df['case_key']
  df['embedding_count'] = df["embedding_count"].map(count_dict)


  # most embedding has no mor ethen 2 repeats, remove 4 3-time embeddings and 1 5-times embeddings
  # only consider embedding less than 3 for now
  df = df[df['embedding_count'] < 3]

  print(df["case_key"].head(2))
  print(df.groupby(by='client').duration_sec.quantile(0.75))
  print(df.groupby(by='client').duration_sec.describe())

  # todo , only take 75% quantile duration cases

  df["client"] = df["client"].astype("category")
  df["case_priority"] = df["case_priority"].astype("category")

  df.drop(df[df.duration_sec <1].index, inplace=True)
  # get first stage ready time, include jobs for today's job


  quantile = dict(df.groupby(by='client').duration_sec.quantile(0.95))
  print('quantile is')
  for key, value in quantile.items():
    print(key, value/60, 'mins')
  del quantile['Embedding']
  paras['95_quantile'] = quantile


  # get first day, second day, thrid day starting jobs

  tmp = df[df.case_stage_rank == 1]

  print(tmp["embedding_count"].value_counts())
  tmp = tmp[tmp.work_ready_timestamp < day_zero].case_key
  todayJobs = list(tmp.unique())
  print(todayJobs)
  print(f'today jobs {len(todayJobs)}')
  paras[day_zero] = todayJobs

  for idx, day_ending in  enumerate(day_endings):
    tmp = df[df.case_stage_rank == 1]
    if idx == 0:
      tmp = tmp[(tmp.work_ready_timestamp < day_ending)].case_key
    else:
      tmp = tmp[(tmp.work_ready_timestamp > day_endings[idx-1]) & (tmp.work_ready_timestamp < day_ending)].case_key
    day_jobs = list(tmp.unique())
    paras['day_jobs'][idx] = day_jobs



  tmp = df[df.case_stage_rank == 1]

  print(tmp["embedding_count"].value_counts())
  tmp = tmp[(tmp.work_ready_timestamp > day_zero) & (tmp.work_ready_timestamp < day_one)].case_key
  todayJobs = list(tmp.unique())
  print(todayJobs)
  print(f'day 1 jobs {len(todayJobs)}')
  paras[day_one] = todayJobs

  tmp = df[df.case_stage_rank == 1]

  print(tmp["embedding_count"].value_counts())
  tmp = tmp[(tmp.work_ready_timestamp > day_one) & (tmp.work_ready_timestamp < day_two)].case_key
  todayJobs = list(tmp.unique())
  print(todayJobs)
  print(f'day 2 jobs {len(todayJobs)}')
  paras[day_two] = todayJobs



  print(df.groupby(by=['client'])['duration_sec'].describe())
  print(df['duration_sec'].min())
  print(df.describe())
  print('case priority summary \n', df['case_priority'].describe())
  clients = df['client'].unique()
  case_priority = df['case_priority'].unique()
  case_key = df['case_key'].unique()

  # make a map
  name_to_idx_key = {name: idx for idx, name in enumerate(case_key)}
  idx_to_name_key = {idx: name for idx, name in enumerate(case_key)}

  name_to_idx_client = {name: idx for idx, name in enumerate(clients)}
  idx_to_name_client = {idx: name for idx, name in enumerate(clients)}

  name_to_idx_priority = {name: idx for idx, name in enumerate(case_priority)}
  idx_to_name_priority = {idx: name for idx, name in enumerate(case_priority)}

  paras[name_to_idx_key_str] = name_to_idx_key
  paras[idx_to_name_key_str] = idx_to_name_key

  paras[name_to_idx_client_str] = name_to_idx_client
  paras[idx_to_name_client_str] = idx_to_name_client

  paras[name_to_idx_priority_str] = name_to_idx_priority
  paras[idx_to_name_priority_str] = idx_to_name_priority

  paras[batch_stage_idx_str] = name_to_idx_client['Embedding']
  paras[nine_hour_priority_idx_str] = name_to_idx_priority['9 Hours']
  paras[two_hour_priority_idx_str] = name_to_idx_priority['2 Hours']
  #print(f'name to idx key {paras[name_to_idx_key_str] }')
  #print(f'idx to name key {paras[idx_to_name_key_str]}')

  print(f'name to idx client {paras[name_to_idx_client_str]}')
  print(f'idx to name client {paras[idx_to_name_client_str]}')

  print(f'name to idx priority {paras[name_to_idx_priority_str] }')
  print(f'idx to name priority {paras[idx_to_name_priority_str]}')

  print (f'embedding indx {paras[batch_stage_idx_str]}')
  print (f'9 hour indx { paras[nine_hour_priority_idx_str]}')

  return df


def row_process(row, day):
  global addedjob
  global job_data
  # let me process one row first
  # for each row, need to put task into a map structure for each case key

  # an array of (client idx, duration)
  case_key_idx = paras[name_to_idx_key_str][row.case_key]

  if paras['full'] == True:
    return
  if row.case_key not in paras['day_jobs'][day]: return

  #print('name = ', name)
  if len(job_data) == paras[max_job_str]:
    print ('job lenth {}'.format(len(job_data)))
    print ('delete {}'.format( paras['last_inserted_case']))
    del job_data[paras['last_inserted_case']]
    paras['full'] = True
    return

  if case_key_idx not in job_data:
    job_data[case_key_idx] = []
  client_idx = paras[name_to_idx_client_str][row.client]
  priority_idx = paras[name_to_idx_priority_str][row.case_priority]
  duration = int(row.duration_sec)
  # reaplce duration if it is above 95% quantile with 95% quantile value
  if row.client in paras['95_quantile']:
    if row.duration_sec > paras['95_quantile'][row.client]:
      duration = int(paras['95_quantile'][row.client])

  task = task_type(client_idx = client_idx, priority_idx=priority_idx ,duration= duration, ready_time=int(row.ready_time_sec), order = row.case_stage_rank)
  job_data[case_key_idx].append(task)
  # newly added job has weight of 1
  paras[job_weights_str][case_key_idx] = 1
  paras['last_inserted_case'] = case_key_idx


def read():
  df = load_real_data()
  return df

def load_new_day(df, day):
  
  
  
  df.apply(row_process, args = (day,), axis=1)
  
  # add floatted job
  if day > 0:
    unfinished = paras['unfinished']
    for case_key_idx, tasks in unfinished.items():
      job_data[case_key_idx] = tasks
      if case_key_idx not in job_data:
        job_data[case_key_idx] = []
      for task in tasks:
        paras[job_weights_str][case_key_idx] = 200

                  
    
  for key in job_data:
    print (f'job {key}')
    examine_case(key, day)

  print(f'nb jobs {len(job_data)}')


df = read()

for day in range(0,2):
  job_data = {}
  paras['full'] = False
  load_new_day(df, day)
  print('total jobs for today {}'.format(len(job_data)))
  lab_model(paras, job_data, day)



df = pd.DataFrame(paras['result'],
                  columns=['case_key', 'client', 'client_ordered', 'start', 'end', 'duration', 'ready_time'])
df[["start", "end", "ready_time"]] = df[["start", "end", "ready_time"]].apply(pd.to_datetime)
df = df.sort_values(["case_key", "client_ordered"], ascending = (False, True))
df.to_csv('out.csv', index=False)
print(df.head)