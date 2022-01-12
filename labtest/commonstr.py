from datetime import datetime
from datetime import timedelta

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

#os.remove("log.txt")

def write_to_file(strs):
    file2 = open(r"log.txt", "a")
    file2.writelines(strs)
    file2.close()
