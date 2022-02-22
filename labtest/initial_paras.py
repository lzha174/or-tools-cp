import collections
#from datetime import datetime
from datetime import timedelta
import datetime

import numpy as np
import pandas as pd
import copy

name_to_idx_key_str = 'name_to_idx_key'
idx_to_name_key_str = 'idx_to_name_key'

name_to_idx_client_str = 'name_to_idx_client'
idx_to_name_client_str = 'idx_to_name_client'

name_to_idx_priority_str = 'name_to_idx_priority'
idx_to_name_priority_str = 'idx_to_name_priority'

batch_stage_idx_str = 'batch_stage_idx'
nine_hour_priority_idx_str = 'nine_hour_priority_idx'
two_hour_priority_idx_str = 'two_hour_priority_idx'

name_to_idx_user_str = 'name_to_idx_user'
idx_to_name_usr_str = 'idx_to_name_user'

stage_to_idx_str = 'name_to_idx_stage'
idx_to_stage_str = 'idx_to_name_stage'

max_job_str = 'max_job'
job_data_dict_str = 'job_data'

job_weights_str = 'job weight'

day_start = '2021-05-17 00:00:00'
day_start_mid = '2021-05-17 14:00:00'
day_zero = '2021-05-18 00:00:00'
day_zero_mid = '2021-05-18 14:00:00'
day_one = '2021-05-19 00:00:00'
day_one_mid = '2021-05-19 14:00:00'
day_two = '2021-05-20 00:00:00'
day_two_mid = '2021-05-20 14:00:00'
day_three = '2021-05-21 00:00:00'
day_three_mid = '2021-05-21 14:00:00'
day_four = '2021-05-21 00:00:00'
day_four_mid = '2021-05-21 14:00:00'
day_five = '2021-05-21 00:00:00'
day_five_mid = '2021-05-21 14:00:00'

morning_str = 'morning'
lunch_str = 'lunch'
workers_str = 'workers'

half_day_pairs = []

# index is patten index, value is start time and ending time
shift_pattern_type = collections.namedtuple('shift', 'start start_str end end_str')

worker_profile_type = collections.namedtuple('profile', 'idx skillset')


# key is shift pattern index, value is staffing for each stage during this period


# I want to create shift patterns every 2 hours from 8am to 6pm

def format_staff_time(value):
    if value < 10:
        value_str = '0{d}:00'.format(d=value)
    else:
        value_str = '{d}:00'.format(d=value)
    return value_str


shift_patterns = {}
staffing = {}
staff_interval = 4
nb_days = 7
nb_shifts = 3
nb_fake_users = 10
nb_days_rostering = 28
fake_user_suffix = 'fake_user_'

for step in range(0, nb_shifts):
    start = 8 + staff_interval * step
    end = start + staff_interval
    if end > 19: end = 18
    start_str = format_staff_time(start)
    end_str = format_staff_time(end)

    shift = shift_pattern_type(start=start, start_str=start_str, end=end, end_str=end_str)
    shift_patterns[step] = shift
    # for now , make staffing same # 5 is preprocess stage before embedding
    staffing[step] = {0: 1, 1: 3, 2:2, 3: 1000, 4: 2, 5: 2}


def staffing_to_csv(duplicate = True):
    output = []
    if duplicate:
        # duplicate this day station demand to every day in this week
        for day in range(28):
            for shift in range(nb_shifts):
                for stage in staffing[shift]:
                    # dont need embedding
                    if stage == paras[batch_stage_idx_str]: continue
                    stage_output = copy.deepcopy(stage)
                    # offset followed stage by -1
                    if stage > paras[batch_stage_idx_str]:
                        stage_output = stage_output - 1

                    value = staffing[shift][stage]
                    data = [day, shift, stage_output, value]
                    output.append(data)


    result_df = pd.DataFrame(output,
                             columns=['day', 'period', 'stage', 'value'])

    write_to_csv(result_df, 'staff.csv')
    return result_df


# how many samples each woker can do each hour
capacity_before_break = {0: 8, 1: 8, 2: 8, 3: 1000, 4: 8, 5: 8}

min_shift_key = 0
max_shift_key = nb_shifts - 1

# create windows for loading data
day_data_windows = {}  # index by day, save array of data windows
for day in range(0, nb_days):
    day_value = 17 + day
    yester_day = day_value - 1
    if day_value == 22:
        day_value = 24
        yester_day = 21
    elif day_value == 23:
        day_value = 25
        yester_day = 24

    data_start_loading = f'2021-05-{day_value}'
    data_finish_loading = f'2021-05-{day_value}'
    data_windows = []
    for key, value in shift_patterns.items():
        if key == 0:
            # first shift, load all data from yesterday's last shift time to this shift's end time
            day_start_value = yester_day
            data_start_loading = ' '.join([f'2021-05-{day_start_value}', shift_patterns[max_shift_key].end_str])
            data_end_loading = ' '.join([f'2021-05-{day_value}', value.end_str])
        else:
            data_start_loading = ' '.join([f'2021-05-{day_value}', shift_patterns[key - 1].end_str])
            data_end_loading = ' '.join([f'2021-05-{day_value}', value.end_str])
        data_windows.append((data_start_loading, data_end_loading))
    day_data_windows[day] = data_windows

for key, data in day_data_windows.items():
    print(f'day {key} data windows {data}')

#  is it possible to know if a worker is busy or idle during an interval?
# in an optimised solution, yes, you can assign new task to next avaliable worker, then measure utility,  in the real world, maybe not?


periods = [morning_str, lunch_str]

day_endings = [day_zero, day_one, day_two, day_three]

# now change to
seconds_per_hour = 60 * 60
day_in_seconds = 24 * 60 * 60


def format_time(n=15):
    # Given timestamp in string
    time_str = '05/17/2021 00:00:00'
    date_format_str = '%m/%d/%Y %H:%M:%S'
    # create datetime object from timestamp string
    given_time = datetime.datetime.strptime(time_str, date_format_str)
    # print('Given timestamp: ', given_time)
    # Add 15 minutes to datetime object
    final_time = given_time + timedelta(seconds=n)
    # print('Final Time (15 minutes after given time ): ', final_time)
    # Convert datetime object to string in specific format
    final_time_str = final_time.strftime('%m/%d/%Y %H:%M:%S')
    # print('Final Time as string object: ', final_time_str)
    return final_time_str


import os


# os.remove("log-increase.txt")
def clear_file(file):
    file = open(file, "r+")
    file.truncate(0)
    file.close()


def write_to_file(filename='log.txt', strs=[]):
    file2 = open(filename, "a")
    file2.writelines(strs)
    file2.close()


# clear_file("log.txt")
# clear_file("solver.txt")

paras = {
    'lunch_used_embeddings': 0,
    'night_used_embeddings': 0,
    'unfinished': {},  # unfinished job from befor b4
    max_job_str: 500,
    'start_emdbedding': {2:12 * seconds_per_hour, 9:20 * seconds_per_hour},
    # start time for category 0 and 1 at stage 2, 12pm, and 6 pm
    'duration_2': {2:2 * seconds_per_hour, 9:9 * seconds_per_hour},  # duration for category 0 and 1 at embedding in seconds

    'max_serach_time_sec': 70,

    job_weights_str: {},
    'result': [],
    'full': False,
}

two_hour_idx = 0
nine_hour_idx = 1
nigh_stage = 20
next_task_type = collections.namedtuple('next_task', 'job_key, task')

root_captioning = ''


def write_to_csv(df, filename):
    filename = os.path.join(root_captioning, filename)
    df.to_csv(filename, index=False)


def load_real_data():
    df = pd.read_csv("5-day-embedding.csv")
    #df = load_from_sql()
    df['start_timestamp'] = pd.to_datetime(df['start_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['end_timestamp'] = pd.to_datetime(df['end_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['work_ready_timestamp'] = pd.to_datetime(df['work_ready_timestamp'], format='%Y-%m-%d %H:%M:%S')
    # https://www.kite.com/python/answers/how-to-find-the-number-of-seconds-between-two-datetime-objects-in-python
    past_date = pd.to_datetime('17/05/2021 00:00')
    df['case_priority'].fillna(1, inplace=True)
    df['duration'] = df['end_timestamp'] - df["start_timestamp"]
    df['duration_sec'] = df['duration'] / np.timedelta64(1, 's')

    df['ready_time_sec'] = df['work_ready_timestamp'].apply(lambda x: (x - past_date).total_seconds())

    f = df[df.case_key == 'ff65e4c1533550f73c6986d629fad87cbb9b75c1aab469bc8b174f15862ee65b']
    print('f is ', f['work_ready_timestamp'].values, f['ready_time_sec'].values)

    # remove jobs that has more than 2 embeddings, first need to count
    # count how many embeeddings for each key
    # first find all rows whose client is embeddings

    df['remove_unfinished'] = False

    # i want to remove cases whose signout is not included in the data
    # first find the last stage client
    f = dict(df.groupby('case_key').case_stage_rank.max())
    # find max value row index?
    # print(f)

    for key, value in f.items():
        # find each subframe
        temp = df[df.case_key == key]
        temp = temp[temp.case_stage_rank == value].client.tolist()
        # print(temp)
        if 'Signout' not in temp:
            df.loc[df.case_key == key, 'remove_unfinished'] = True
            # print(f'remove unfinished case {key}')
    df.drop(df[df.remove_unfinished == True].index, inplace=True)

    f = df[df.client == 'Embedding']
    f = f.groupby(by='case_key').client.value_counts()
    count_dict = {}
    # create a dict of case_key, embedding value count
    for i, v in f.iteritems():
        # print('index: ', i[0], 'value: ', v)
        count_dict[i[0]] = v
        # if v>=3: print (f'remove case {i}')

    df['embedding_count'] = df['case_key']
    df['embedding_count'] = df["embedding_count"].map(count_dict)

    # most embedding has no mor ethen 2 repeats, remove 4 3-time embeddings and 1 5-times embeddings
    # only consider embedding less than 3 for now
    df.drop(df[df['embedding_count'] >= 3].index, inplace=True)

    # df.to_csv('filtered_data.csv', index=False)
    write_to_csv(df, 'filtered_data.csv')

    print(df["case_key"].head(2))
    print(df.groupby(by='client').duration_sec.quantile(0.75))
    print(df.groupby(by='client').duration_sec.describe())

    # todo , only take 75% quantile duration cases

    # df["client"] = df["client"].astype("category")
    # df["case_priority"] = df["case_priority"].astype("category")

    # df.drop(df[df.duration_sec < 1].index, inplace=True)
    df.loc[(df.duration_sec < 1), 'duration_sec'] = 1

    # get first stage ready time, include jobs for today's job

    quantile = dict(df.groupby(by='client').duration_sec.quantile(0.75))
    print('quantile is')
    for key, value in quantile.items():
        print(key, value / 60, 'mins')
    paras['95_quantile'] = quantile

    # get first day, second day, thrid day starting jobs
    # save jobs belong to each shift window
    # is it ok to load tasks cant finish during this window? yes, coz end time constraint will make sure each task fall into the window
    paras['jobs_in_shift'] = {}
    tmp = df[df.case_stage_rank == 1]
    case_arrival_times = dict(tmp.groupby(by='case_key').work_ready_timestamp.min())
    for key, data_windows in day_data_windows.items():
        # index of data_window is same as shift_patterns
        for idx, data_window in enumerate(data_windows):

            df['arrival_time'] = df['case_key']
            df['arrival_time'] = df["arrival_time"].map(case_arrival_times)
            paras['jobs_in_shift'][key, idx] = df[(df.arrival_time >= data_window[0]) & (df.arrival_time < data_window[1])]

    # get ready_time for the first stage
    min_ready_time_series = dict(tmp.groupby(by='case_key').ready_time_sec.min())

    paras['min_ready_time_series'] = min_ready_time_series

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

    print('total null is', df.isna().sum())
    f = df[df['embedding_count'].isna()].case_key.unique()
    print(f)

    return df

useNoteBook = False

def python_min(a,b):
    return None

def python_max(a,b):
    return None

def custom_min(a, b):
    if useNoteBook:
        return python_min(a, b)
    else:
        return min(a,b)

def custom_max(a,b):
    if useNoteBook:
        return python_max(a, b)
    else:
        return max(a,b)
