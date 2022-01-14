import collections

from ortools.sat.python import cp_model

from drawing import draw

import pandas as pd
import numpy as np
from shift_model import *
from commonstr import *
import openpyxl

task_type = collections.namedtuple('task', 'client_idx priority_idx duration ready_time order first_task_ready_time')

job_data = {}  # key by key_idx, inside need to store tasks in order which may have same tasks at different stage

paras = {
    'lunch_used_embeddings': 0,
    'night_used_embeddings': 0,
    'unfinished': {},  # unfinished job from befor b4
    max_job_str: 500 ,
    'days': 5,
    'start': 8,  # start time for non embedding stage
    'end': 21.5,  # end time for non embedding stage,  8pm - 5am
    'start_emdbedding': [12 * seconds_per_hour, 20 * seconds_per_hour],
    # start time for category 0 and 1 at stage 2, 12pm, and 6 pm
    'duration_2': [2 * seconds_per_hour, 9 * seconds_per_hour],  # duration for category 0 and 1 at embedding in seconds

    'max_serach_time_sec': 480,
    'capacity': {0: 3, 1: 12, 2: 1000, 3: 8, 4: 4},
    job_weights_str: {},
    'result': [],
    'full': False,
    'day_jobs': {}
}

# i want to find max work time at evening

def examine_case(case_idx, day_index=0):
    print(paras[idx_to_name_key_str][case_idx])
    tasks = job_data[case_idx]
    for idx, task in enumerate(tasks):

        client = paras[idx_to_name_client_str][task.client_idx]
        priority = paras[idx_to_name_priority_str][task.priority_idx]
        duration = task.duration
        ready = format_time(task.ready_time)
        print(f'client {client} priority {priority} duration {duration} ready {ready} ready {task.ready_time}')

        pd_ready_time = pd.to_datetime(ready)
        pd_day_end = pd.to_datetime(day_endings[day_index])
        if (idx == 0):
            assert (pd_ready_time < pd_day_end)


def load_real_data():
    df = pd.read_csv("5-day.csv")
    df['start_timestamp'] = pd.to_datetime(df['start_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['end_timestamp'] = pd.to_datetime(df['end_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['work_ready_timestamp'] = pd.to_datetime(df['work_ready_timestamp'], format='%Y-%m-%d %H:%M:%S')
    # https://www.kite.com/python/answers/how-to-find-the-number-of-seconds-between-two-datetime-objects-in-python
    print(df.work_ready_timestamp.head(3))

    print(df.head(3))
    past_date = pd.to_datetime('17/05/2021 00:00')
    df['case_priority'].fillna(1)
    df['duration'] = df['end_timestamp'] - df["start_timestamp"]
    df['duration_sec'] = df['duration'] / np.timedelta64(1, 's')

    df['ready_time_sec'] = df['work_ready_timestamp'].apply(lambda x: (x - past_date).total_seconds())

    f = df[df.case_key == 'ff65e4c1533550f73c6986d629fad87cbb9b75c1aab469bc8b174f15862ee65b']
    print('f is ', f['work_ready_timestamp'].values, f['ready_time_sec'].values)

    # remove jobs that has more than 2 embeddings, first need to count
    # count how many embeeddings for each key
    # first find all rows whose client is embeddings


    f = df[df.client == 'Embedding']
    f = f.groupby(by='case_key').client.value_counts()
    count_dict = {}
    # create a dict of case_key, embedding value count
    for i, v in f.iteritems():
        # print('index: ', i[0], 'value: ', v)
        count_dict[i[0]] = v
        if v>=3: print (f'remove case {i}')

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

    #df.drop(df[df.duration_sec < 1].index, inplace=True)
    df.loc[(df.duration_sec < 1), 'duration_sec'] = 1

    temp = df[df.case_key == 'dccd1e14b9e9c6e0217bce756b92e643e500223dfa1bedd2d955c71d0bb6ca5b']
    print(temp['client'])
    print(temp['work_ready_timestamp'])
    print(temp['case_stage_rank'])
    print(temp['duration_sec'])


    # get first stage ready time, include jobs for today's job

    quantile = dict(df.groupby(by='client').duration_sec.quantile(0.95))
    print('quantile is')
    for key, value in quantile.items():
        print(key, value / 60, 'mins')
    del quantile['Embedding']
    paras['95_quantile'] = quantile

    # get first day, second day, thrid day starting jobs
    # save jobs belong to each shift window
    # is it ok to load tasks cant finish during this window? yes, coz end time constraint will make sure each task fall into the window
    for key, data_windows in day_data_windows.items():
        # index of data_window is same as shift_patterns
        for idx, data_window in enumerate(data_windows):
            tmp = df[df.case_stage_rank == 1]
            tmp = tmp[
                (tmp.work_ready_timestamp >= data_window[0]) & (tmp.work_ready_timestamp < data_window[1])].case_key
            period_jobs = list(tmp.unique())
            paras['day_jobs'][key, idx] = period_jobs

    tmp = df[df.case_stage_rank == 1]
    # get ready_time for the first stage
    min_ready_time_series = dict(tmp.groupby(by='case_key').ready_time_sec.min())
    paras['min_ready_time_series'] = min_ready_time_series
    for i, v in min_ready_time_series.items():
        print (f'case {i} min ready_time is {v}' + ' ' + format_time(v))

    for idx, half_pair in enumerate(half_day_pairs):
        # idx is day index
        for period, data_range in half_pair.items():
            tmp = df[df.case_stage_rank == 1]
            tmp = tmp[
                (tmp.work_ready_timestamp > data_range[0]) & (tmp.work_ready_timestamp < data_range[1])].case_key
            period_jobs = list(tmp.unique())
            if 'dccd1e14b9e9c6e0217bce756b92e643e500223dfa1bedd2d955c71d0bb6ca5b' in period_jobs:
                print('fuck')
            paras['day_jobs'][idx, period] = period_jobs







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
    # print(f'name to idx key {paras[name_to_idx_key_str] }')
    # print(f'idx to name key {paras[idx_to_name_key_str]}')

    print(f'name to idx client {paras[name_to_idx_client_str]}')
    print(f'idx to name client {paras[idx_to_name_client_str]}')

    print(f'name to idx priority {paras[name_to_idx_priority_str]}')
    print(f'idx to name priority {paras[idx_to_name_priority_str]}')

    print(f'embedding indx {paras[batch_stage_idx_str]}')
    print(f'9 hour indx {paras[nine_hour_priority_idx_str]}')

    print('total null is', df.isna().sum().sum())
    return df


case_max_stages = {}

bench_ready_time = {}
bench_finish_time = {}
min_ready_times = {}

def row_process(row, day, period):
    global addedjob
    global job_data
    # let me process one row first
    # for each row, need to put task into a map structure for each case key

    # an array of (client idx, duration)

    case_key_idx = paras[name_to_idx_key_str][row.case_key]

    if paras['full'] == True:
        return
    x = paras['day_jobs'].get((day, period), None)
    if x is None: return
    if row.case_key not in paras['day_jobs'][day, period]: return
    if row.case_key == 'dccd1e14b9e9c6e0217bce756b92e643e500223dfa1bedd2d955c71d0bb6ca5b':
        print(row.case_key)
    # print('name = ', name)
    if len(job_data) == paras[max_job_str]:
        print('job lenth {}'.format(len(job_data)))
        print('delete {}'.format(paras['last_inserted_case']))
        del job_data[paras['last_inserted_case']]
        paras['full'] = True
        return

    if case_key_idx not in job_data:
        job_data[case_key_idx] = []
    client_idx = paras[name_to_idx_client_str][row.client]
    priority_idx = paras[name_to_idx_priority_str][row.case_priority]
    duration = int(row.duration_sec)
    case_stage_rank = row.case_stage_rank

    if case_key_idx not in bench_ready_time:
        bench_ready_time[case_key_idx] = row.work_ready_timestamp
        bench_finish_time[case_key_idx] = row.end_timestamp
        min_ready_times[case_key_idx] = row.ready_time_sec
    else:
        bench_ready_time[case_key_idx] = min(row.work_ready_timestamp, bench_ready_time[case_key_idx])
        bench_finish_time[case_key_idx] = max(row.end_timestamp, bench_finish_time[case_key_idx])
        min_ready_times[case_key_idx] = min(row.ready_time_sec,min_ready_times[case_key_idx])

    if case_key_idx not in case_max_stages:
        case_max_stages[case_key_idx] = case_stage_rank
    else:
        case_max_stages[case_key_idx] = max(case_max_stages[case_key_idx], case_stage_rank)
    # reaplce duration if it is above 95% quantile with 95% quantile value
    if row.client in paras['95_quantile']:
        if row.duration_sec > paras['95_quantile'][row.client]:
            duration = int(paras['95_quantile'][row.client])
    min_ready_time =  int(paras['min_ready_time_series'][row.case_key])
    task = task_type(client_idx=client_idx, priority_idx=priority_idx, duration=duration,
                     ready_time=min_ready_time, order=row.case_stage_rank, first_task_ready_time = min_ready_time)
    job_data[case_key_idx].append(task)
    # newly added job has weight of 1
    paras[job_weights_str][case_key_idx] = 1
    paras['last_inserted_case'] = case_key_idx


def read():
    df = load_real_data()
    return df


def load_new_day(df, day, period):
    df.apply(row_process, args=(day, period,), axis=1)
    print(f'new jobs {len(job_data)}')
    # add floatted job
    if len(paras['unfinished']) > 0:
        unfinished = paras['unfinished']
        for case_key_idx, tasks in unfinished.items():
            job_data[case_key_idx] = tasks
            if case_key_idx not in job_data:
                job_data[case_key_idx] = []
            for task in tasks:
                paras[job_weights_str][case_key_idx] = 200
    return
    for key in job_data:
        print(f'job {key}')
        examine_case(key, day)

    print(f'nb jobs {len(job_data)}')


df = read()
def solve():
    global job_data
    for day, data_windows in day_data_windows.items():
        paras['night_used_embeddings'] = 0
        paras['lunch_used_embeddings'] = 0
        # index of data_window is same as shift_patterns
        for idx, data_window in enumerate(data_windows):
            job_data = {}
            paras['full'] = False
            load_new_day(df, day, idx)
            print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            if (len(job_data) == 0): continue


            shift_model(paras, job_data, day, idx)


def solve_period():
    global job_data
    for day, data_windows in day_data_windows.items():
        if day >= 1: continue
        # index of data_window is same as shift_patterns
        for idx, data_window in enumerate(data_windows):
            job_data = {}
            paras['full'] = False
            load_new_day(df, day, idx)
            print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            if (len(job_data) == 0): continue


            shift_model(paras, job_data, day, idx)
    exit(1)

solve()
#solve_period()



df = pd.DataFrame(paras['result'],
                  columns=['case_key', 'case_name', 'client', 'case_stage_rank', 'start', 'end', 'duration', 'ready_time'])
df[["start", "end", "ready_time"]] = df[["start", "end", "ready_time"]].apply(pd.to_datetime)
df = df.sort_values(["case_key", "case_stage_rank"], ascending=(False, True))
df.to_csv('out.csv', index=False)
print(df.head)

finished_case = []


def add_finished_stats(row):
    case_key_index = row.case_key
    case_stage_rank = row.case_stage_rank
    if case_max_stages[case_key_index] == case_stage_rank:
        finished_case.append(case_key_index)


f = df[df.case_stage_rank == 1]
f = f.groupby(by='case_key').ready_time.min()

count_dict = {}
# create a dict of case_key, embedding value count
for i, v in f.iteritems():
    #print('index: ', i, 'value: ', v)
    count_dict[i] = v


df['min_ready_time'] = df['case_key']
df['min_ready_time'] = df["min_ready_time"].map(count_dict)
df['min_ready_time'] = pd.to_datetime(df['min_ready_time'])

#df.to_csv('readty.csv', index =False)
# do stats after warmup and b4 cool down periord, all jobs whose ready time bewteen thewse are considered finished?
stats_start_ready_date  = '2021-05-18 00:00:00'
stat_end_ready_date  = '2021-05-21 00:00:00'
stats_df = df[(df.min_ready_time >= stats_start_ready_date) & (df.min_ready_time < stat_end_ready_date)]
stats_df = stats_df.sort_values(["case_key", "case_stage_rank"], ascending=(True, True))




# how to i calculate total in-system time for each case
# group by case_key, find minimum ready_time, find maximum end_time, difference is the total in system time
tmp = stats_df[stats_df['client'] == 'Signout']
tmp.apply(add_finished_stats, axis=1)
# get case_key

print('finished case', finished_case)
finished = stats_df[stats_df['case_key'].isin(finished_case)]
finished.to_csv('stats_out.csv', index = False)

# the last stage is signout

ready_time_df = finished.groupby(by='case_key').ready_time.min()
max_end_time_df = finished.groupby(by='case_key').end.max()

print(max_end_time_df, ready_time_df)
print(max_end_time_df - ready_time_df)

total_duration_df = pd.DataFrame(max_end_time_df - ready_time_df, columns=['optimised_duration'])
print('or average',total_duration_df['optimised_duration'].mean())


bench_duration = {}
for key in finished_case:
    bench_duration[key] = bench_finish_time[key] - bench_ready_time[key]
bench_duration_df = pd.DataFrame.from_dict(bench_duration, orient='index', columns=['bench_duration'])
print('bench mean value',bench_duration_df.mean())



result = pd.concat([total_duration_df, bench_duration_df], axis=1, join='inner')
result.to_csv('duration_compare.csv')