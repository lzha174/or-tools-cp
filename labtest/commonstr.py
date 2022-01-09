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

day_zero = '2021-05-18 00:00:00'
day_one = '2021-05-19 00:00:00'
day_two = '2021-05-20 00:00:00'

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
