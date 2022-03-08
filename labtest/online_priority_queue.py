# importing "bisect" for bisection operations
from collections import deque
import bisect
# from utilisation_plot import *
from online_opt_schedulingclass import *

task_type = collections.namedtuple('task',
                                   'case_key_idx client_idx priority_idx duration ready_time order first_task_ready_time task_rank')

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

counter = 0
task_ranks = [0, 1]
paras['job_rank'] = {}


def row_process(row, day, period):
    global addedjob
    global job_data
    global counter, task_ranks
    # let me process one row first
    # for each row, need to put task into a map structure for each case key

    # an array of (client idx, duration)
    # if row.remove_unfinished == True: return

    if paras['full'] == True:
        return
    # x = paras['day_jobs'].get((day, period), None)
    # if x is None: return
    # if row.case_key not in paras['day_jobs'][day, period]: return

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

    if case_key_idx not in paras['job_rank']:
        # paras['job_rank'][case_key_idx] = task_ranks[counter % 2]
        paras['job_rank'][case_key_idx] = 0
        counter = counter + 1
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
                     ready_time=min_ready_time, order=row.case_stage_rank, first_task_ready_time=min_ready_time,
                     task_rank=paras['job_rank'][case_key_idx])
    job_data[case_key_idx].append(task)
    # newly added job has weight of 1
    paras[job_weights_str][case_key_idx] = 1
    paras['last_inserted_case'] = case_key_idx


def load_new_day(day, period):
    paras['jobs_in_shift'][day, period].apply(row_process, args=(day, period,), axis=1)
    # df.apply(row_process, args=(day, period,), axis=1)
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
    end_date = start_date + 4
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

    finished = stats_df[stats_df['case_key'].isin(finished_case)]
    # finished = finished[finished['client'] == 'Grossing'].sort_values(["start"], ascending=(True))
    # finished.to_csv('stats_out.csv', index=False)
    write_to_csv(finished, 'stats_out.csv')

    temp = stats_df[stats_df.case_stage_rank == 1].sort_values(["start"], ascending=(True))
    case_key = temp['case_key'].tolist()

    for i in range(len(case_key) - 1):
        print(case_key[i], temp[temp.case_key == case_key[i]].end.values[0], case_key[i+1],temp[temp.case_key == case_key[i + 1]].start.values[0])
        assert( temp[temp.case_key == case_key[i]].end.values[0] <= temp[temp.case_key == case_key[i + 1]].start.values[0] )

    # the last stage is signout

    ready_time_df = finished.groupby(by='case_key').ready_time.min()
    max_end_time_df = finished.groupby(by='case_key').end.max()

    # print(max_end_time_df, ready_time_df)
    # print(max_end_time_df - ready_time_df)

    total_duration_df = pd.DataFrame(max_end_time_df - ready_time_df, columns=['optimised_duration'])
    print('or average', total_duration_df['optimised_duration'].mean())

    target_time = datetime.timedelta(days=2)
    total_duration_df['targetOffset'] = total_duration_df['optimised_duration'] - target_time
    total_duration_df['onTarget'] = total_duration_df['targetOffset']
    total_duration_df.loc[total_duration_df['targetOffset'] <= datetime.timedelta(), 'onTarget'] = 1
    total_duration_df.loc[total_duration_df['targetOffset'] > datetime.timedelta(), 'onTarget'] = 0

    logstr.append('average {} \n'.format(total_duration_df['optimised_duration'].mean()))
    print(total_duration_df.info())
    print('ontargt', total_duration_df['onTarget'].sum() / float(len(total_duration_df)))
    total_duration_df.to_csv('simresult.csv', index=False)

    # write_to_file('log.txt', staffingconfig)

    bench_duration = {}
    for key in finished_case:
        bench_duration[key] = bench_finish_time[key] - bench_ready_time[key]
    bench_duration_df = pd.DataFrame.from_dict(bench_duration, orient='index', columns=['bench_duration'])
    print('bench mean value', bench_duration_df.mean())

    result = pd.concat([total_duration_df, bench_duration_df], axis=1, join='inner')
    # result.to_csv('duration_compare.csv')
    # write_to_csv(result, 'duration_compare.csv')

    utilisation_df = pd.DataFrame(paras['utilisation'],
                                  columns=['day', 'period', 'client', 'utilisation'])
    utilisation_df.to_csv('utilisation.csv', index=False)
    write_to_csv(utilisation_df, 'utilisation.csv')
    print('finished case', len(finished_case))
    total_cases_to_collect = 0
    for day in range(start_date, end_date):
        stats_start_ready_date = f'2021-05-{day} 00:00:00'

        stat_end_ready_date = f'2021-05-{day + 1} 00:00:00'

        f = df[df.case_stage_rank == 1]
        f = f[(f.work_ready_timestamp > stats_start_ready_date) & (f.work_ready_timestamp < stat_end_ready_date)]
        # how many jobs in this day
        print(stats_start_ready_date, stat_end_ready_date, 'total jobs', len(f.index))

        f_finished = stats_df[stats_df.case_stage_rank == 1]
        f_finished = f_finished[
            (f_finished.min_ready_time > stats_start_ready_date) & (f_finished.min_ready_time < stat_end_ready_date)]
        print(stats_start_ready_date + ' jobs that are in the scheduling ', len(f_finished.index))
        total_cases_to_collect = total_cases_to_collect + len(f.index)
        # I am happy if a few of them is not finished in my rolling window for local serach
        # assert (len(finished_case) > 0.9 * len(f_finished))
        # finished_percentage = len(finished_case) / (1.0* len(f_finished))
        # print('finish percentage is ', finished_percentage)
    # check if 90% is finished within two days
    reach_target = False
    # if finished_percentage >= 0.95 and total_duration_df['optimised_duration'].mean() - timedelta(days = 2) <timedelta(seconds = 1):
    #    reach_target = True
    print('finisihed percetage', len(finished_case) / float(total_cases_to_collect))
    # print(total_duration_df['optimised_duration'].mean() - timedelta(days = 2) < timedelta(hours = 10))
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


# now refactor so that they use the same code
# make a array of queues by ranking, process them in order

def create_queues():
    # pretend 2 ranks, rank 0 is higher than rank 1
    queue_array = []
    for i in range(2):
        task_queue = deque([])
        queue_from_last_shift = deque([])
        queue_array.append((task_queue, queue_from_last_shift))
    return queue_array


# every shift period, need to initilise the queue
def initialise_queue(queue_from_last_shift, rank):
    # create a copy
    task_queue = copy.deepcopy(queue_from_last_shift)
    for job, tasks in job_data.items():
        new_job = JobClass()
        new_job.set_job(job, tasks)
        first_task = new_job.get_next_task()
        if first_task.task_rank == rank:
            # print(new_job)
            allJobs.add_job(new_job)

            bisect.insort_right(task_queue, first_task)
    return task_queue


def process_queue(day_index, period, task_queue, queue_from_last_shift):
    # the question is how to schedule jobs with priority......
    # if I schule high priorityh jobs first, how to then insert low pirority jobs into free times bewteen high pirorities...
    # this will be a total different way of finding a worker...if everything is processed in natual time order, I don't need to consider this problem
    # when I schuedue a low priority job, go through each worker, find all time itervals between high prioirty jobs it has done, if there is one to fit, put this job in there
    # and do not need to udate next avalible job time
    # if there is no such interval, append this to the end of  worker schedule with earliest avalible time

    # empty empty from last shift
    queue_from_last_shift.clear()
    ends_time = int(shift_patterns[period].end * seconds_per_hour + day_index * day_in_seconds)
    if period == max_shift_key:
        next_period = 0
        next_shift_day = day_index + 1
    else:
        next_period = period + 1
        next_shift_day = day_index
    next_shift_start_time = int(shift_patterns[next_period].start * seconds_per_hour + next_shift_day * day_in_seconds)
    while len(task_queue) > 0:
        task = task_queue.popleft()

        next_task = None
        if task.client_idx != paras[batch_stage_idx_str]:
            has_task_to_process = False
            # if process this task pass the shift end time, we push this task to the next shift initilised queue and continue to next task

            while has_task_to_process is False:
                if task.ready_time + task.duration >= ends_time:
                    bisect.insort_right(queue_from_last_shift, task)
                    if len(task_queue) == 0: break
                    task = task_queue.popleft()

                else:
                    has_task_to_process = True
            if has_task_to_process == False:
                break
            # find a worker
            job_id = task.job_id
            if job_id == 1349 or job_id == 266:
                print('im here')
            if (task.job_id == 1349) and task.client_idx == 0:
                print('stage', task.client_idx, task)
            # print(task)
            worker_reschedule = stage_workers[task.client_idx].reschedule(task, task.duration)
            max_task_assigned = 0
            best_worker = None
            rescheduled_tasks = None
            new_available_time = None
            for key, value in worker_reschedule.items():
                if len(value[0]) == 0 : continue

                nb_assigned, all_new_tasks = key.reschedule(task, value)

                if nb_assigned > max_task_assigned:
                    max_task_assigned =  nb_assigned
                    best_worker = key
                    rescheduled_tasks = all_new_tasks
                    new_available_time = value[1]
            if best_worker is not None:
                unscheduled, scheduled = best_worker.fix_reschedule(allJobs, rescheduled_tasks, new_available_time)
                if len(unscheduled) > 0:
                    for t in unscheduled:
                        #print('unschedued', t)
                        # remove the next task in the queue
                        current_task = allJobs[t.job_id].current_task()
                        if current_task in task_queue:
                            task_queue.remove(current_task)
                        bisect.insort_right(queue_from_last_shift, t)
                if len(scheduled) > 0:
                    # resort the queue with updated ready time for next task
                    for t in scheduled:
                        #print(task)
                        next_task =  allJobs[t.job_id].get_next_task()
                        #print('next t ask', next_task)
                        # rescheduled task, need to reinsert its next task with updated ready time
                        if next_task in task_queue:
                            task_queue.remove(next_task)
                            bisect.insort_right(task_queue, next_task )
                        else:
                            if next_task is not None:
                                bisect.insort_right(task_queue, next_task )
                        #for i in task_queue:
                        #    print('in queue', i)



        else:
            priority = task.priority
            if priority == paras[nine_hour_priority_idx_str]:
                # allJobs.show_job(next_task.job_key)
                # put this into night batch
                # print(stage_workers[nigh_stage])
                duration = paras['duration_2'][9]
                worker, task_start_time = stage_workers[nigh_stage].insert_into_idle(task, duration)
                # worker = stage_workers[nigh_stage].next_avaliable_worker(task.get_ready_time(),
                #                                                         duration)
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
                worker, task_start_time = stage_workers[task.client_idx].insert_into_idle(task,
                                                                                          duration)
                # worker = stage_workers[task.client_idx].next_avaliable_worker(task.get_ready_time(),
                #                                                    duration)

                if worker is None:
                    # passed lunch batch, go for night
                    duration = paras['duration_2'][9]
                    worker, task_start_time = stage_workers[nigh_stage].insert_into_idle(task,
                                                                                         duration)

                    if worker is not None:
                        paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                else:
                    paras['lunch_used_embeddings'] = paras['lunch_used_embeddings'] + 1

            if worker is not None:
                # print(next_task.task)
                # task_start_time = custom_max(worker.get_avaliable_time(), task.get_ready_time())
                # need to update embedding duration based on lunch or night
                task.duration = duration
                # worker.update_last_task_finish_time(task.get_ready_time(), duration)
                job = allJobs.mark_job_task_finish(task.job_id, task_start_time)
                next_task = job.get_next_task()
                # print('next task is ', next_task)
            else:
                bisect.insort_right(queue_from_last_shift, task)

            if next_task is not None:
                # if next_task.job_id == 1805 and next_task.client_idx == 1:
                #   print(next_task)
                bisect.insort_right(task_queue, next_task)

        # print('after procesd')
        # for task in high_rank_task_queue:
        #    print(task)

    if day_index >= 0 and day_index <= 3:

        if period == max_shift_key:
            utilisation = [day_index, period]
            stage_name = 'lunch_embedding'
            average_utilisation = paras['lunch_used_embeddings'] / 1000.0
            percentage = "{:.0%}".format(average_utilisation)
            utilisation.append(stage_name)
            utilisation.append(percentage)
            paras['utilisation'].append(utilisation)

            utilisation = [day_index, period]
            stage_name = 'night_embedding'
            average_utilisation = paras['night_used_embeddings'] / 1000.0
            percentage = "{:.0%}".format(average_utilisation)
            utilisation.append(stage_name)
            utilisation.append(percentage)
            paras['utilisation'].append(utilisation)

        for key in stage_workers:
            utilisation = [day_index, period]
            if key == paras[batch_stage_idx_str] or key == nigh_stage: continue

            stage_name = paras[idx_to_name_client_str][key]

            average_utilisation = stage_workers[key].average_utilisation()
            percentage = "{:.0%}".format(average_utilisation)
            utilisation.append(stage_name)
            utilisation.append(percentage)
            paras['utilisation'].append(utilisation)


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
    paras['staffing'] = current_staffing
    paras['result'] = []
    paras['utilisation'] = []
    current_day = 17 + day_index_local

    data_rolling_window_lower_bound = day_index_local - 1
    data_rolling_window_upper_bound = day_index_local + 1

    paras[workers_str] = {}  # key day, period, stage, value is a collection of workers for that shift

    # each rank has a qeue
    queue_array = create_queues()
    global job_data
    for day, data_windows in day_data_windows.items():
        a = datetime.datetime.now()
        # if day < data_rolling_window_lower_bound: continue
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
            print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            # if (len(job_data) == 0): continue

            define_workers(day, idx)

            # process jobs in ranking order
            for rank in range(1):
                task_queue = queue_array[rank][0]
                queue_from_last_shift = queue_array[rank][1]

                # copy first task of job data and unifnished tasks from last shift into this queue
                task_queue = initialise_queue(queue_from_last_shift, rank)
                # start process
                process_queue(day, idx, task_queue, queue_from_last_shift)

                if True:
                    print('queue are ')
                    print(len(queue_from_last_shift))

        b = datetime.datetime.now()
        print('time elapsed is,', b - a)

        # get_break_stats()

        # print('todao job is ', job_today)
        # print('night used', paras['night_used_embeddings'])
        # print('lunch used', paras['lunch_used_embeddings'])
    # get_gantt()
    allJobs.get_result()
    average_time, reach_target = record_result(current_day)

    # return average_time, reach_target


# assign_model(staffing)

def get_gantt():
    selected_day = 0
    selected_period = 0
    selected_stage = 0
    selected_worker = 0

    # lets build a data frame
    data = []
    for key in paras[workers_str]:
        day = key[0]
        period = key[1]
        stage = key[2]
        if stage == paras[batch_stage_idx_str] or stage == nigh_stage: continue
        data = data + paras[workers_str][key].get_gantt()

    result_df = pd.DataFrame(data,
                             columns=['day', 'period', 'stage', 'worker', 'start', 'duration'])

    # result_df.to_csv('worker_gantt.csv', index=False)
    write_to_csv(result_df, 'worker_gantt.csv')

    # i want to draw gantt chart for stage 2 worker at day 1, period 1
    sliced_df = result_df[(result_df.day == selected_day) & (result_df.period == selected_period) &
                          (result_df.stage == selected_stage) & (result_df.worker == selected_worker)][
        ['start', 'duration']]
    starts_time = int(shift_patterns[selected_period].start * seconds_per_hour + selected_day * day_in_seconds)
    ends_time = starts_time + 5 * seconds_per_hour
    sliced_df = sliced_df[(sliced_df.start >= starts_time) & (sliced_df.start < ends_time)]
    print('sliced nb are ', len(sliced_df))
    sliced_df['bar4'] = sliced_df.values.tolist()
    output = sliced_df['bar4'].values.tolist()


a = datetime.datetime.now()
# repeat_local_search()
assign_model(staffing, 0)
# shift_local_search(1)
# staffing_to_csv(True)
b = datetime.datetime.now()
# print('time elapsed is,', b - a)

