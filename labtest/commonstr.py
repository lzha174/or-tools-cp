from datetime import datetime
from datetime import timedelta
import collections

name_to_idx_key_str = 'name_to_idx_key'
idx_to_name_key_str = 'idx_to_name_key'

name_to_idx_client_str = 'name_to_idx_client'
idx_to_name_client_str = 'idx_to_name_client'

name_to_idx_priority_str = 'name_to_idx_priority'
idx_to_name_priority_str = 'idx_to_name_priority'

batch_stage_idx_str = 'batch_stage_idx'
nine_hour_priority_idx_str = 'nine_hour_priority_idx'
two_hour_priority_idx_str = 'two_hour_priority_idx'

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

half_day_pairs = []
for day in range(0,7):

    date = 17 + day
    next_day = date + 1
    s = f'2021-05-{date} 00:00:00'
    s_mid = f'2021-05-{date} 14:00:00'
    s_next = f'2021-05-{next_day} 00:00:00'
    value = {}
    value[morning_str] = (s, s_mid)
    value[lunch_str] = (s_mid, s_next)
    half_day_pairs.append(value)
# index is patten index, value is start time and ending time
shift_pattern_type = collections.namedtuple('shift', 'start start_str end end_str')
shift_patterns = {0:shift_pattern_type(start=6, start_str='06:00', end=10, end_str='10:00'), 1: shift_pattern_type(start=10, start_str='10:00',
                                                                                                                   end=14, end_str='14:00'),
                  2:shift_pattern_type(start=14, start_str='14:00', end=16, end_str='16:00' )}


# key is shift pattern index, value is staffing for each stage during this period

staffing = {0: {0: 3, 1:12, 2:1000, 3:8, 4:4}, 1:{0: 3, 1:12, 2:1000, 3:8, 4:4}, 2:{0: 3, 1:5, 2:1000, 3:6, 4:4}}

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
nb_days = 5
for day in range(nb_days):
    for step in range(0,3):
        start = 8 + staff_interval*step
        end = start + staff_interval
        if end > 19: end = 18
        start_str = format_staff_time(start)
        end_str = format_staff_time(end)

        shift = shift_pattern_type(start=start, start_str=start_str, end=end, end_str = end_str)
        shift_patterns[step] = shift
        # for now , make staffing same
        staffing[day, step] = {0: 1, 1:2, 2:1000, 3:2, 4:2}



# how many samples each woker can do each hour
capacity_before_break = {0: 6, 1:6, 2:1000, 3: 6, 4:6}

min_shift_key = min(shift_patterns)
max_shift_key = max(shift_patterns)

# create windows for loading data
day_data_windows = {} # index by day, save array of data windows
for day in range(0,nb_days):
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
            data_start_loading = ' '.join([f'2021-05-{day_value}', shift_patterns[key-1].end_str])
            data_end_loading = ' '.join([f'2021-05-{day_value}', value.end_str])
        data_windows.append((data_start_loading, data_end_loading))
    day_data_windows[day] = data_windows

for key, data in day_data_windows.items():
    print (f'day {key} data windows {data}')

#  is it possible to know if a worker is busy or idle during an interval?
# in an optimised solution, yes, you can assign new task to next avaliable worker, then measure utility,  in the real world, maybe not?


periods = [morning_str, lunch_str]

day_endings = [day_zero, day_one, day_two, day_three]


# now change to
seconds_per_hour = 60 * 60
day_in_seconds = 24 * 60 * 60

def format_time(n = 15):
    # Given timestamp in string
    time_str = '05/17/2021 00:00:00'
    date_format_str = '%m/%d/%Y %H:%M:%S'
    # create datetime object from timestamp string
    given_time = datetime.strptime(time_str, date_format_str)
    #print('Given timestamp: ', given_time)
    # Add 15 minutes to datetime object
    final_time = given_time + timedelta(seconds=n)
    #print('Final Time (15 minutes after given time ): ', final_time)
    # Convert datetime object to string in specific format
    final_time_str = final_time.strftime('%m/%d/%Y %H:%M:%S')
    #print('Final Time as string object: ', final_time_str)
    return  final_time_str

import os

#os.remove("log-increase.txt")
def clear_file(file):

    file = open(file,"r+")
    file.truncate(0)
    file.close()
def write_to_file(filename = 'log.txt', strs=[]):
    file2 = open(filename, "a")
    file2.writelines(strs)
    file2.close()

#clear_file("log.txt")
#clear_file("solver.txt")

paras = {
    'lunch_used_embeddings': 0,
    'night_used_embeddings': 0,
    'unfinished': {},  # unfinished job from befor b4
    max_job_str: 460,
    'days': 5,
    'start': 8,  # start time for non embedding stage
    'end': 21.5,  # end time for non embedding stage,  8pm - 5am
    'start_emdbedding': [12 * seconds_per_hour, 20 * seconds_per_hour],
    # start time for category 0 and 1 at stage 2, 12pm, and 6 pm
    'duration_2': [2 * seconds_per_hour, 9 * seconds_per_hour],  # duration for category 0 and 1 at embedding in seconds

    'max_serach_time_sec': 70,
    'capacity': {0: 3, 1: 12, 2: 1000, 3: 8, 4: 4},
    job_weights_str: {},
    'result': [],
    'full': False,
    'day_jobs': {}
}

two_hour_idx = 0
nine_hour_idx = 1
nigh_stage = 20
next_task_type = collections.namedtuple('next_task', 'job_key, task')