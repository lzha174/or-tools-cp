# for each stage loop throuch each worker, find the earliest avaiable worker to grab the next task in the list
# for the selected worker, based on its avaliable time, find the best work
# we quite the loop if no worker is avalible in the shift period, or no task is avaiable for that stage any more
# basically we run an infintie loop
# the whole thing is changed.....
import threading
import time

from heuristic_scheduling_class import *

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
embedding_count = {}


def row_process(row, day, period):
    global addedjob
    global job_data
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
    embedding_count[case_key_idx] = row.embedding_count

    if case_key_idx not in job_data:
        job_data[case_key_idx] = []
    client_idx = paras[name_to_idx_client_str][row.client]
    priority_idx = paras[name_to_idx_priority_str][row.case_priority]
    if case_key_idx == 1233:
        priority_idx = 1
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


def load_job_into_pool(current_pool):
    global job_data

    for job, tasks in job_data.items():
        new_job = JobClass()
        embedding = embedding_count[job]
        new_job.set_job(job, embedding, tasks)
        # print(new_job)
        allJobs.add_job(new_job)


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
        # assert (len(finished_case) > 0.9 * len(f_finished))
        finished_percentage = len(finished_case) / (1.0 * len(f_finished))
        for f in f_finished.case_key.unique():
            if f not in finished_case:
                print('unfinished', f)
        print('finish percentage is ', finished_percentage)
    # check if 90% is finished within two days
    reach_target = False
    if finished_percentage >= 0.95 and total_duration_df['optimised_duration'].mean() - timedelta(days=2) < timedelta(
            seconds=1):
        reach_target = True

    print(total_duration_df['optimised_duration'].mean() - timedelta(days=2) < timedelta(hours=10))
    return total_duration_df['optimised_duration'].mean(), reach_target


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
        mid_day_time = 12 * seconds_per_hour + day_index * day_in_seconds
        lunch_start_time = 13 * seconds_per_hour + day_index * day_in_seconds
        lunch_end_time = 14 * seconds_per_hour + day_index * day_in_seconds

        if stage != paras[batch_stage_idx_str]:
            paras[workers_str][day_index, period, stage] = WokrerCollection(day_index, period, stage, capacity[stage],
                                                                            starts_time, ends_time, mid_day_time,
                                                                            lunch_start_time,
                                                                            lunch_end_time)
            stage_workers[stage] = paras[workers_str][day_index, period, stage]
            # print(stage_workers[stage])

            # for embedding we need two stages, one for lunch one for night


def define_jobs(current_job_pool):
    global job_data
    global allJobs

    for job, tasks in job_data.items():
        new_job = JobClass()
        embedding = embedding_count[job]
        new_job.set_job(job, embedding, tasks)
        # print(new_job)
        allJobs.add_job(new_job)
        current_job_pool.append(new_job)
    # allJobs.next_task_stage(0)


def key_combiner(*keyfuncs):
    def helper(elem):
        return [keyfunc(elem) for keyfunc in keyfuncs]

    return helper


def sortByReadyTime(elem):
    return elem.ready_time


def sortByArriveTime(elem):
    return elem.first_task_ready_time


# we sort in asec order, meaning [0, 4] is before [1,2]
# so before mid day, a 2 hour task give a value of 0 in terms of prority, which will be taken first if possible
def sortByPrority(worker):
    def helper(elem):
        # check how many batching left to do
        job = allJobs[elem.job_id]
        if job.batch_count > 0:
            # if pass mid day, 9 hour has higier priority
            if worker.last_task_finish_time > worker.mid_day:
                if elem.priority != paras[two_hour_priority_idx_str]:
                    # 9 hour batch is more important
                    return 0
                else:
                    return 1
            else:
                # before mid day, 2 hour has higher prioity
                if elem.priority == paras[two_hour_priority_idx_str]:
                    return 0
                else:
                    return 1
        else:
            return 1

    return helper


def pull_task(current_job_pool, stage, worker):
    # find the earliest job this worker can do
    worker_free_time = worker.last_task_finish_time

    # get the most important task
    candiate_tasks_in_the_queue = []
    next_task_to_arrive = None
    for job in current_job_pool:
        current_task = job.current_task()
        if current_task.client_idx == stage:
            if current_task.ready_time < worker_free_time:
                candiate_tasks_in_the_queue.append(current_task)
            else:
                # use the next arrived task in time order
                if next_task_to_arrive is None:
                    next_task_to_arrive = current_task
                else:
                    if current_task.ready_time < next_task_to_arrive.ready_time:
                        next_task_to_arrive = current_task
    if len(candiate_tasks_in_the_queue) > 0:
        # try to grab the most important task in the queue

        sortedTasks = sorted(candiate_tasks_in_the_queue,
                             key=key_combiner(sortByPrority(worker),
                                              sortByReadyTime, sortByArriveTime))

        # for s in sortedTasks:
        #    print(s)
        return sortedTasks[0]
    else:
        return next_task_to_arrive
    # sort the task in ready_time


def put_task_to_batch(task, duration, batch_bin):
    w = stage_workers[batch_bin].get_idle_batch_worker()
    if w is not None:
        w.fill_task()
        # update task
        job_id = task.job_id
        allJobs[job_id].reduce_batch_count()
        allJobs[job_id].move_to_next_task(task_start_time=w.start_time, duration=w.duration)
        return True
    return False


def check_is_batch(current_job_pool, wait_pool, next_task):
    if next_task.client_idx == paras[batch_stage_idx_str]:
        priority = next_task.priority
        if priority == paras[two_hour_priority_idx_str]:

            if put_task_to_batch(next_task, paras['duration_2'][2], paras[mid_day_batch_str]):
                # mark this is ok

                return
            elif put_task_to_batch(next_task, paras['duration_2'][9], paras[night_batch_str]):
                # mark this is ok

                return
            else:
                # queue this task for next day batch
                job_id = next_task.job_id
                current_job_pool.remove(allJobs[job_id])
                wait_pool.append(allJobs[job_id])
        else:
            # 9 hour priority
            #print(priority)
            if put_task_to_batch(next_task, paras['duration_2'][9], paras[night_batch_str]):
                # mark this is ok

                return
            else:
                # queue this task for next day
                job_id = next_task.job_id
                current_job_pool.remove(allJobs[job_id])
                wait_pool.append(allJobs[job_id])


def pull_job(current_job_pool):
    wait_pool = []
    # for each stage, find the worker that become avalible earliest
    # we have a pool of jobs for this shift, if a job cant be processed, remove this job from the pool and put it into waiting list for the next shift
    print('current pool size', len(current_job_pool))
    # while len(current_job_pool) > 0:
    # for each stage, find the earliest avalible worker, check the pool, if there is a task for that stage and this worker can do this task in the shift,
    # assign the task, move to next task
    # if no worker can do this task in the shift, remove this job to the waiting pool for next shift
    # for each stage worker, check the pool, if there is a task do it. if a task cant be done by any worker, remove the job from the pool
    # now lets try to pull some task
    while len(current_job_pool) > 0:
        for stage, group in stage_workers.items():
            for w in group.workers:
                if stage != paras[batch_stage_idx_str] and stage != nigh_stage:
                    task_to_do = pull_task(current_job_pool, stage, w)
                    print('next task is ', task_to_do)

                    if task_to_do is None:
                        break
                    job_id = task_to_do.job_id
                    if job_id == 1147:
                        print('g')
                    # now check if this worker can do this task
                    eligible, start_time, end_time = w.free_to_next_task(task_to_do)
                    if eligible:
                        w.update_free_time(start_time=start_time, end_time=end_time, task=task_to_do)
                        # print(eligible, format_time(start_time))
                        # print(w)
                        # update job next task ready time

                        allJobs[job_id].move_to_next_task(start_time)
                        # print(allJobs[job_id])

                        if allJobs[job_id].job_done:
                            current_job_pool.remove(allJobs[job_id])
                            # print(f'remove {job_id}')
                        else:
                            next_task = allJobs[job_id].current_task()

                            check_is_batch(current_job_pool=current_job_pool, wait_pool=wait_pool, next_task=next_task)
                    else:
                        # if this task is beyond shift tifirst_task_ready_timeme, ?
                        first_free_w = group.earliest_free_worker()
                        eligible, start_time, end_time = first_free_w.free_to_next_task(task_to_do)
                        if not eligible:
                            if job_id == 110:
                                print('f')
                            # print('trying to remove', job_id)
                            current_job_pool.remove(allJobs[job_id])
                            wait_pool.append(allJobs[job_id])

            # sort workers by free time
            group.sort_workers()

    for stage, group in stage_workers.items():
        group.check_overlap()
    return wait_pool


def define_batch_workers(day_index):
    # for each day we have two batches slots
    # if shift starts befor mid day, define mid day batch

    period = 0
    capacity = paras['staffing'][period]
    stage = paras[mid_day_batch_str]
    starts_time = paras['start_emdbedding'][2] + day_index * day_in_seconds
    ends_time = starts_time + paras['duration_2'][2]
    mid_day_time = 12 * seconds_per_hour + day * day_in_seconds
    lunch_start_time = 0
    lunch_end_time = 0
    paras[workers_str][day_index, period, stage] = BatchCollection(day_index, period, stage,
                                                                   capacity[stage],
                                                                   starts_time, ends_time, mid_day_time,
                                                                   lunch_start_time,
                                                                   lunch_end_time)

    stage_workers[stage] = paras[workers_str][day_index, period, stage]

    stage = paras[night_batch_str]
    starts_time = paras['start_emdbedding'][9] + day_index * day_in_seconds
    ends_time = starts_time + paras['duration_2'][9]

    paras[workers_str][day_index, period, stage] = BatchCollection(day_index, period, stage,
                                                                   capacity[paras[batch_stage_idx_str]],
                                                                   starts_time, ends_time, mid_day_time,
                                                                   lunch_start_time, lunch_end_time)

    stage_workers[stage] = paras[workers_str][day_index, period, stage]  # print(stage_workers[stage])


def pull_from_pool(stage, current_job_pool, wait_pool):
    group = stage_workers[stage]
    stop = len(current_job_pool) == 0
    while not stop:
        #print('current pool size', len(current_job_pool))
        for w in group.workers:
            if stop:
                return
            time.sleep(0.001)
            with lock:
                #print('stage is', stage)
                if stage != paras[batch_stage_idx_str] and stage != nigh_stage:

                    task_to_do = pull_task(current_job_pool, stage, w)
                    #print('next task is ', task_to_do)

                    if task_to_do is None:
                        if len(current_job_pool) == 0:
                            stop = True
                        break
                    if task_to_do is not None:
                        job_id = task_to_do.job_id
                        if job_id == 1147:
                            print('g')
                        # now check if this worker can do this task
                        eligible, start_time, end_time = w.free_to_next_task(task_to_do)
                        if eligible:
                            w.update_free_time(start_time=start_time, end_time=end_time, task=task_to_do)
                            # print(eligible, format_time(start_time))
                            # print(w)
                            # update job next task ready time

                            allJobs[job_id].move_to_next_task(start_time)
                            # print(allJobs[job_id])

                            if allJobs[job_id].job_done:
                                current_job_pool.remove(allJobs[job_id])
                                # print(f'remove {job_id}')
                            else:
                                next_task = allJobs[job_id].current_task()

                                check_is_batch(current_job_pool=current_job_pool, wait_pool=wait_pool, next_task=next_task)
                        else:
                            # if this task is beyond shift tifirst_task_ready_timeme, ?
                            first_free_w = group.earliest_free_worker()
                            eligible, start_time, end_time = first_free_w.free_to_next_task(task_to_do)
                            if not eligible:
                                if job_id == 110:
                                    print('f')
                                # print('trying to remove', job_id)
                                current_job_pool.remove(allJobs[job_id])
                                wait_pool.append(allJobs[job_id])


def create_thread(current_job_pool):
    threads_list = []
    wait_pool = []
    for stage in stage_workers:
        if stage != paras[batch_stage_idx_str] and stage != nigh_stage:
            new_thread = threading.Thread(target=pull_from_pool, args=(stage, current_job_pool, wait_pool))
            threads_list.append(new_thread)
    for i in threads_list:
        i.start()
    for i in threads_list:
        i.join()
    return wait_pool


# now try to assign jobs to workers at each stage
def assign_model(current_staffing, day_index_local=1):
    paras['staffing'] = current_staffing
    paras['result'] = []
    paras['utilisation'] = []
    current_day = 17 + day_index_local

    data_rolling_window_lower_bound = day_index_local - 1
    data_rolling_window_upper_bound = day_index_local + 4

    paras[workers_str] = {}  # key day, period, stage, value is a collection of workers for that shift
    current_job_pool = []
    wait_pool = []

    global job_data
    for day, data_windows in day_data_windows.items():
        if day < data_rolling_window_lower_bound: continue
        if day > data_rolling_window_upper_bound: break
        job_today = 0
        paras['night_used_embeddings'] = 0
        paras['lunch_used_embeddings'] = 0
        # index of data_window is same as shift_patterns
        define_batch_workers(day)
        for idx, data_window in enumerate(data_windows):
            job_data = {}
            paras['full'] = False
            load_new_day(day, idx)
            job_today = job_today + len(job_data)
            # need to know the starting job pool, this pool will lose a job if that job cant be processed given the job ready time

            print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            # if (len(job_data) == 0): continue
            current_job_pool.clear()
            current_job_pool = wait_pool
            define_jobs(current_job_pool)
            define_workers(day, idx)
            wait_pool = create_thread(current_job_pool)


        # get_break_stats()
        # print('todao job is ', job_today)
        # print('night used', paras['night_used_embeddings'])
        # print('lunch used', paras['lunch_used_embeddings'])
    # get_gantt()
    average_time, reach_target = record_result(current_day)
    return average_time, reach_target


# assign_model(staffing)
lock = threading.Lock()

a = datetime.datetime.now()
# repeat_local_search()
assign_model(staffing, 0)
# shift_local_search(1)
# staffing_to_csv(True)
b = datetime.datetime.now()
print('time elapsed is,', b - a)
