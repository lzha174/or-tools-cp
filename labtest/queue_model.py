# importing "bisect" for bisection operations
from collections import deque
import bisect

from schedulingclasses import *

task_type = collections.namedtuple('task',
                                   'case_key_idx client_idx priority_idx duration ready_time order first_task_ready_time')

job_data = {}  # key by key_idx, inside need to store tasks in order which may have same tasks at different stage


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


case_max_stages = {}
last_client = {}

bench_ready_time = {}
bench_finish_time = {}
min_ready_times = {}


def row_process(row, day, period):
    global addedjob
    global job_data
    # let me process one row first
    # for each row, need to put task into a map structure for each case key

    # an array of (client idx, duration)
    # if row.remove_unfinished == True: return

    if paras['full'] == True:
        return
    #x = paras['day_jobs'].get((day, period), None)
    #if x is None: return
    #if row.case_key not in paras['day_jobs'][day, period]: return

    # print('name = ', name)
    if len(job_data) == paras[max_job_str]:
        print('job lenth {}'.format(len(job_data)))
        print('delete {}'.format(paras['last_inserted_case']))
        del job_data[paras['last_inserted_case']]
        paras['full'] = True
        return
    case_key_idx = paras[name_to_idx_key_str][row.case_key]

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
        # notebook chagne to python_min
        bench_ready_time[case_key_idx] = custom_min(row.work_ready_timestamp, bench_ready_time[case_key_idx])
        bench_finish_time[case_key_idx] = custom_max(row.end_timestamp, bench_finish_time[case_key_idx])
        min_ready_times[case_key_idx] = custom_min(row.ready_time_sec, min_ready_times[case_key_idx])


    if case_key_idx not in case_max_stages:
        case_max_stages[case_key_idx] = case_stage_rank
    else:
        case_max_stages[case_key_idx] = custom_max(case_max_stages[case_key_idx], case_stage_rank)
    # reaplce duration if it is above 95% quantile with 95% quantile value
    if row.client in paras['95_quantile']:
        if row.duration_sec > paras['95_quantile'][row.client]:
            duration = int(paras['95_quantile'][row.client])
    min_ready_time = int(paras['min_ready_time_series'][row.case_key])

    task = task_type(case_key_idx=case_key_idx, client_idx=client_idx, priority_idx=priority_idx, duration=duration,
                     ready_time=min_ready_time, order=row.case_stage_rank, first_task_ready_time=min_ready_time)
    job_data[case_key_idx].append(task)
    # newly added job has weight of 1
    paras[job_weights_str][case_key_idx] = 1
    paras['last_inserted_case'] = case_key_idx


def load_new_day(day, period):
    paras['jobs_in_shift'][day, period].apply(row_process, args=(day, period,), axis=1)
    #df.apply(row_process, args=(day, period,), axis=1)
    # print(f'new jobs {len(job_data)}')
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


df = load_real_data()


def add_finished_stats(row, finished_case):
    case_key_index = row.case_key
    case_stage_rank = row.case_stage_rank
    if case_max_stages[case_key_index] == case_stage_rank:
        finished_case.append(case_key_index)


def record_result(start_date=18):
    logstr = []
    end_date = start_date + 1
    result_df = pd.DataFrame(paras['result'],
                             columns=['case_key', 'case_name', 'priority', 'client', 'case_stage_rank', 'start', 'end',
                                      'duration', 'ready_time'])
    result_df[["start", "end", "ready_time"]] = result_df[["start", "end", "ready_time"]].apply(pd.to_datetime)
    result_df = result_df.sort_values(["case_key", "case_stage_rank"], ascending=(False, True))
    # result_df.to_csv('out.csv', index=False)
    write_to_csv(result_df, 'out.csv')
    print(result_df.head)
    print('im here ')

    finished_case = []

    f = result_df[result_df.case_stage_rank == 1]
    f = f.groupby(by='case_key').ready_time.min()

    count_dict = {}
    # create a dict of case_key, embedding value count
    for i, v in f.iteritems():
        # print('index: ', i, 'value: ', v)
        count_dict[i] = v

    result_df['min_ready_time'] = result_df['case_key']
    result_df['min_ready_time'] = result_df["min_ready_time"].map(count_dict)
    result_df['min_ready_time'] = pd.to_datetime(result_df['min_ready_time'])

    # df.to_csv('readty.csv', index =False)
    # do stats after warmup and b4 cool down periord, all jobs whose ready time bewteen thewse are considered finished?
    stats_start_ready_date = f'2021-05-{start_date} 00:00:00'
    stat_end_ready_date = f'2021-05-{end_date} 00:00:00'
    stats_df = result_df[
        (result_df.min_ready_time >= stats_start_ready_date) & (result_df.min_ready_time < stat_end_ready_date)]
    stats_df = stats_df.sort_values(["case_key", "case_stage_rank"], ascending=(True, True))

    # how to i calculate total in-system time for each case
    # group by case_key, find minimum ready_time, find maximum end_time, difference is the total in system time
    # some jobs may not end yet, the sign is that the last stage is finished, it is a signout
    tmp = stats_df[stats_df['client'] == 'Signout']
    tmp.apply(add_finished_stats, args=(finished_case,), axis=1)
    # get case_key

    print('finished case', len(finished_case))
    finished = stats_df[stats_df['case_key'].isin(finished_case)]
    # finished.to_csv('stats_out.csv', index=False)
    write_to_csv(finished, 'stats_out.csv')

    # the last stage is signout

    ready_time_df = finished.groupby(by='case_key').ready_time.min()
    max_end_time_df = finished.groupby(by='case_key').end.max()

    # print(max_end_time_df, ready_time_df)
    # print(max_end_time_df - ready_time_df)

    total_duration_df = pd.DataFrame(max_end_time_df - ready_time_df, columns=['optimised_duration'])
    print('or average', total_duration_df['optimised_duration'].mean())
    logstr.append('average {} \n'.format(total_duration_df['optimised_duration'].mean()))

    # write_to_file('log.txt', staffingconfig)

    bench_duration = {}
    for key in finished_case:
        bench_duration[key] = bench_finish_time[key] - bench_ready_time[key]
    bench_duration_df = pd.DataFrame.from_dict(bench_duration, orient='index', columns=['bench_duration'])
    print('bench mean value', bench_duration_df.mean())

    result = pd.concat([total_duration_df, bench_duration_df], axis=1, join='inner')
    # result.to_csv('duration_compare.csv')
    write_to_csv(result, 'duration_compare.csv')

    utilisation_df = pd.DataFrame(paras['utilisation'],
                                  columns=['day', 'period', 'client', 'utilisation'])
    # utilisation_df.to_csv('utilisation.csv', index=False)
    write_to_csv(utilisation_df, 'utilisation.csv')
    for day in range(start_date, end_date):
        stats_start_ready_date = f'2021-05-{day} 00:00:00'

        stat_end_ready_date = f'2021-05-{day + 1} 00:00:00'

        f = df[df.case_stage_rank == 1]
        f = f[(f.work_ready_timestamp > stats_start_ready_date) & (f.work_ready_timestamp < stat_end_ready_date)]
        # how many jobs in this day
        print(stats_start_ready_date, 'total jobs', len(f.index))

        f_finished = stats_df[stats_df.case_stage_rank == 1]
        f_finished = f_finished[
            (f_finished.min_ready_time > stats_start_ready_date) & (f_finished.min_ready_time < stat_end_ready_date)]
        print(stats_start_ready_date + ' jobs that are in the scheduling ', len(f_finished.index))
        # I am happy if a few of them is not finished in my rolling window for local serach
        #assert (len(finished_case) > 0.9 * len(f_finished))
        finished_percentage = len(finished_case) / (1.0* len(f_finished))
        #print('finish percentage is ', finished_percentage)
    # check if 90% is finished within two days
    reach_target = False
    if finished_percentage >= 0.95 and total_duration_df['optimised_duration'].mean() - timedelta(days = 2) <timedelta(seconds = 1):
        reach_target = True

    #print(total_duration_df['optimised_duration'].mean() - timedelta(days = 2) < timedelta(hours = 10))
    return total_duration_df['optimised_duration'].mean(), reach_target


# write_to_file(logstr)


# go through the job list, find the next task to assign for each stage ( the one with earliest arrival time)
# remove this task from the list, record some stats.....
#  this is beyond my understanding at the moment
#  for a task, find the earliest to be freed worker at each stage.
#  change the avaliable time for this wokrer after finish this task, move on
# just go through the first task in a job , put them into stage bucket in a sorted order?
# given i have a job dict, what to do?

# for each day/period  I need to define stage workers, but batch worker only define once for each day?
# nah I just need to remove used capacity
# note:for now igonre this

stage_workers = {}

allJobs = JobCollection()


def get_break_stats():
    for stage in paras[idx_to_name_client_str]:
        if stage != paras[batch_stage_idx_str]:
            breaks = stage_workers[stage].get_break_stats()
            print(f'for stage {stage} breaks are {breaks}')


def define_workers(day_index=0, period=2):
    # define workers for each stage
    global stage_workers
    capacity = paras['staffing'][period]

    for stage in paras[idx_to_name_client_str]:
        starts_time = int(shift_patterns[period].start * seconds_per_hour + day_index * day_in_seconds)
        ends_time = int(shift_patterns[period].end * seconds_per_hour + day_index * day_in_seconds)
        lunch_start_time = 13 * seconds_per_hour + day_index * day_in_seconds
        lunch_end_time = 14 * seconds_per_hour + day_index * day_in_seconds

        if stage != paras[batch_stage_idx_str]:
            paras[workers_str][day_index, period, stage] = WokrerCollection(day_index, period, stage, capacity[stage],
                                                                            starts_time, ends_time, lunch_start_time,
                                                                            lunch_end_time)
            stage_workers[stage] = paras[workers_str][day_index, period, stage]
            # print(stage_workers[stage])
        else:
            capacity_used = paras['lunch_used_embeddings']
            starts_time = paras['start_emdbedding'][2] + day_index * day_in_seconds
            ends_time = starts_time + paras['duration_2'][2]
            paras[workers_str][day_index, period, stage] = WokrerCollection(day_index, period, stage,
                                                                            capacity[stage] - capacity_used,
                                                                            starts_time, ends_time, lunch_start_time,
                                                                            lunch_end_time)

            stage_workers[stage] = paras[workers_str][day_index, period, stage]  # print(stage_workers[stage])

            capacity_used = paras['night_used_embeddings']
            starts_time = paras['start_emdbedding'][9] + day_index * day_in_seconds
            ends_time = starts_time + paras['duration_2'][9]

            paras[workers_str][day_index, period, nigh_stage] = WokrerCollection(day_index, period, nigh_stage,
                                                                                 capacity[stage] - capacity_used,
                                                                                 starts_time, ends_time,
                                                                                 lunch_start_time, lunch_end_time)

            stage_workers[nigh_stage] = paras[workers_str][day_index, period, nigh_stage]  # print(stage_workers[stage])

            # for embedding we need two stages, one for lunch one for night

task_queue = deque([])
queue_from_last_shift = deque([])
def initilise_queue():
    global task_queue, queue_from_last_shift
    task_queue = copy.deepcopy(queue_from_last_shift)
    for job, tasks in job_data.items():
        new_job = JobClass()
        new_job.set_job(job, tasks)
        # print(new_job)
        allJobs.add_job(new_job)
        first_task = new_job.get_next_task()
        bisect.insort_right(task_queue, first_task)

    #for task in task_queue:
    #    print(task)

def process_queue(day_index, period):
    global task_queue, queue_from_last_shift
    queue_from_last_shift = deque([])
    ends_time = int(shift_patterns[period].end * seconds_per_hour + day_index * day_in_seconds)
    while len(task_queue) > 0:
        task = task_queue.popleft()
        job_id = task.job_id
        stage = task.client_idx
        next_task = None
        if stage != paras[batch_stage_idx_str]:
            has_task_to_process = False
            # if process this task pass the shift end time, we push this task to the next shift initilised queue and continue to next task

            while has_task_to_process is False:
                if task.ready_time + task.duration >= ends_time:
                    queue_from_last_shift.append(task)
                    if len(task_queue) == 0: break
                    task = task_queue.popleft()

                else:
                    has_task_to_process = True
            if has_task_to_process == False: return
            # find a worker
            worker = stage_workers[stage].next_avaliable_worker(task.get_ready_time(), task.duration)
            if worker is not None:
                task_start_time = custom_max(worker.get_avaliable_time(), task.get_ready_time())
                worker.update_last_task_finish_time(task.get_ready_time(), task.duration)
                # print('after assign')
                # print(worker)
                # mark this task finished for that job set the task interval
                job = allJobs.mark_job_task_finish(task.job_id, task_start_time)
                # append next task into the queue
                next_task = job.get_next_task()

            # if there is no worker for this task in this shift, append this task to next shift
            else:
                queue_from_last_shift.append(task)
        else:
            priority = task.priority
            worker = None
            if priority == paras[nine_hour_priority_idx_str]:
                # allJobs.show_job(next_task.job_key)
                # put this into night batch
                # print(stage_workers[nigh_stage])
                duration = paras['duration_2'][9]
                worker = stage_workers[nigh_stage].next_avaliable_worker(task.get_ready_time(),
                                                                         duration)
                ready_time = format_time(task.get_ready_time())
                # print(worker, ready_time, next_task.task.duration)

                if worker is not None:
                    paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                # print('next avaliable worker')
                # print(worker)
            else:
                duration = paras['duration_2'][2]
                # 2 hour prioity, put it into lunch batch if possible
                # get next avalaible worker from two hour batch
                worker = stage_workers[stage].next_avaliable_worker(task.get_ready_time(),
                                                                    duration)

                if worker is None:
                    # passed lunch batch, go for night
                    duration = paras['duration_2'][9]
                    worker = stage_workers[nigh_stage].next_avaliable_worker(task.get_ready_time(),
                                                                             duration)

                    if worker is not None:
                        paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                else:
                    paras['lunch_used_embeddings'] = paras['lunch_used_embeddings'] + 1

            if worker is not None:
                # print(next_task.task)
                task_start_time = custom_max(worker.get_avaliable_time(), task.get_ready_time())
                # need to update embedding duration based on lunch or night
                task.duration = duration
                worker.update_last_task_finish_time(task.get_ready_time(), duration)
                job = allJobs.mark_job_task_finish(task.job_id, task_start_time)
                next_task = job.get_next_task()
                #print('next task is ', next_task)
            else:
                queue_from_last_shift.append(task)
        #print('after procesd')
        if next_task is not None:
            bisect.insort_right(task_queue, next_task)
        #for task in task_queue:
        #    print(task)

def define_jobs():
    global job_data
    global allJobs

    for job, tasks in job_data.items():
        new_job = JobClass()
        new_job.set_job(job, tasks)
        # print(new_job)
        allJobs.add_job(new_job)
    # allJobs.next_task_stage(0)




# now try to assign jobs to workers at each stage
def assign_model(current_staffing, day_index_local=1):
    global task_queue, queue_from_last_shift

    paras['staffing'] = current_staffing
    paras['result'] = []
    paras['utilisation'] = []
    current_day = 17 + day_index_local

    data_rolling_window_lower_bound = day_index_local - 1
    data_rolling_window_upper_bound = day_index_local + 3

    paras[workers_str] = {}  # key day, period, stage, value is a collection of workers for that shift

    global job_data
    for day, data_windows in day_data_windows.items():
        if day < data_rolling_window_lower_bound: continue
        if day > data_rolling_window_upper_bound: break
        job_today = 0
        paras['night_used_embeddings'] = 0
        paras['lunch_used_embeddings'] = 0
        # index of data_window is same as shift_patterns
        for idx, data_window in enumerate(data_windows):
            job_data = {}
            paras['full'] = False
            load_new_day(day, idx)
            job_today = job_today + len(job_data)
            #print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            # if (len(job_data) == 0): continue
            initilise_queue()
            define_workers(day, idx)
            process_queue(day, idx)

            # get_break_stats()

            # print('todao job is ', job_today)
        # print('night used', paras['night_used_embeddings'])
        # print('lunch used', paras['lunch_used_embeddings'])
    # get_gantt()
    average_time, reach_target = record_result(current_day)
    #return average_time, reach_target


# assign_model(staffing)

# i want to define a simple local search for a day's shift? fix all other day's shift
def shift_local_search(day=1):
    paras['result'] = []
    paras['utilisation'] = []
    paras['staffing'] = None
    nb_staff_to_add = 5

    logstr = []

    current_average, reach_target = assign_model(staffing, day)
    print(f'current average {current_average}')
    logstr.append(f'current average before adding staff for day {day} is {current_average}')
    for i in range(nb_staff_to_add):
        outstr = f'add staff {i}\n'
        logstr.append(outstr)
        # say i want to add one more staff, i want to know add to which stage
        stage_to_search = [0, 1, 2, 4, 5]
        # improvement = {}

        # add 1 worker to the best shift, in each shift , find the best stage
        max_improvment_shift = 0
        best_shift = None
        best_average_shift = None
        best_shift_stage = None
        reach_target = False
        for shift_period in range(max_shift_key + 1):
            logstr.append(f'go through shift {shift_period}\n')

            max_improve_all_stages_this_shift = -1e15
            best_average_this_shift = None
            best_stage_this_shift = None

            # given a shift period, find the best stage to add a worker

            for s in stage_to_search:
                temp_staffing = copy.deepcopy(staffing)

                temp_staffing[shift_period][s] = temp_staffing[shift_period][s] + 1
                # print('current staffing', temp_staffing)
                new_average, reach_target = assign_model(temp_staffing, day)
                # when 95% are finished within 2 days, we are done
                improvement = current_average - new_average
                to_seconds = improvement.total_seconds()
                print('to seconds', to_seconds)
                if to_seconds > 0 and to_seconds > max_improve_all_stages_this_shift:
                    max_improve_all_stages_this_shift = to_seconds
                    best_stage_this_shift = s
                    best_average_this_shift = new_average
                    # if we have reached performance targert, no need to continue to add?
                    if reach_target:
                        break
            # after going through all stages of a shift period, if the best improvement is better than current best,
            # mark this shift to be the best shfit to add 1 more staff
            if max_improve_all_stages_this_shift > max_improvment_shift:
                max_improvment_shift = max_improve_all_stages_this_shift
                best_shift = shift_period
                best_average_shift = best_average_this_shift
                best_shift_stage = best_stage_this_shift
                if reach_target:
                    staffing[best_shift][best_shift_stage] = staffing[best_shift][best_shift_stage] + 1
                    return


        if best_shift is not None:
            logstr.append(f'best shift is {best_shift} best stage is {best_shift_stage}\n')
            logstr.append(f'best improvment is {best_average_shift}\n')
            staffing[best_shift][best_shift_stage] = staffing[best_shift][best_shift_stage] + 1
            current_average = best_average_shift
            print(current_average)
        else:
            return

        for shift_period in range(max_shift_key + 1):
            for s in stage_to_search:
                logstr.append(
                    f'period {shift_period} stage {s} staff are {staffing[shift_period][s]}\n')

    write_to_file('log.txt', logstr)

a = datetime.datetime.now()
# repeat_local_search()
assign_model(staffing, 1)
#shift_local_search(1)
#staffing_to_csv(True)
b = datetime.datetime.now()
print('time elapsed is,', b - a)
