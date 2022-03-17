# for each stage loop throuch each worker, find the earliest avaiable worker to grab the next task in the list
# for the selected worker, based on its avaliable time, find the best work
# we quite the loop if no worker is avalible in the shift period, or no task is avaiable for that stage any more
# basically we run an infintie loop
# the whole thing is changed.....
import copy

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
        print('finish percentage is ', finished_percentage)
    # check if 90% is finished within two days
    reach_target = False
    if finished_percentage >= 0.95 and total_duration_df['optimised_duration'].mean() - timedelta(days = 2) <timedelta(seconds = 1):
        reach_target = True

    print(total_duration_df['optimised_duration'].mean() - timedelta(days = 2) < timedelta(hours = 10))
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
        mid_day_time = 12 * seconds_per_hour + day_index * day_in_seconds
        lunch_start_time = 13 * seconds_per_hour + day_index * day_in_seconds
        lunch_end_time = 14 * seconds_per_hour + day_index * day_in_seconds

        if stage != paras[batch_stage_idx_str]:
            paras[workers_str][day_index, period, stage] = WokrerCollection(day_index, period, stage, capacity[stage],
                                                                            starts_time, ends_time, mid_day_time, lunch_start_time,
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

# we sort in asec order, meaning [0, 4] is before [1,2]
# so before mid day, a 2 hour task give a value of 0 in terms of prority, which will be taken first if possible
def sortByPrority(worker):
  def helper(elem):
    # check how many batching left to do
    job = allJobs[elem.job_id]
    if job.embedding_count > 0:
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
  return helper


def pull_batch_tasks(current_job_pool, worker):
    # find the earliest job this worker can do
    worker_free_time = worker.last_task_finish_time

    # get the most important task
    candiate_tasks_in_the_queue = []
    next_task_to_arrive = None
    for job in current_job_pool:
        current_task = job.current_task()
        if current_task.client_idx == paras[batch_stage_idx_str]:

            # this is a batch task, check the worker
            worker_stage = worker.stage
            task_priority = current_task.priority



            # 2 hour priority work can go to both mid day and night batch
            # 9 hour canot go to midday worker batch
            if task_priority == paras[two_hour_priority_idx_str]:



                if worker_stage == paras[batch_stage_idx_str]:
                    if current_task.ready_time < worker_free_time:
                        candiate_tasks_in_the_queue.append(current_task)
                    else:
                        # todo: this task passed midday batch, we really should remove this job from the pool
                        pass
                else: # a night batch worker try to find work
                    # a two hour task should try go to 2 hour batch

                    # a night batch task, we cant process this task

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
                                                 sortByReadyTime))

        for s in sortedTasks:
            print(s)
        return s[0]
    else:
        return next_task_to_arrive
    # sort the task in ready_time




def pull_job(current_job_pool):
    # for each stage, find the worker that become avalible earliest
    # we have a pool of jobs for this shift, if a job cant be processed, remove this job from the pool and put it into waiting list for the next shift
    print('current pool size', len(current_job_pool))
    #while len(current_job_pool) > 0:
        # for each stage, find the earliest avalible worker, check the pool, if there is a task for that stage and this worker can do this task in the shift,
        # assign the task, move to next task
        # if no worker can do this task in the shift, remove this job to the waiting pool for next shift
    # for each stage worker, check the pool, if there is a task do it. if a task cant be done by any worker, remove the job from the pool
    # now lets try to pull some task
    for stage, group in stage_workers.items():
        for w in group.workers:
            if stage != paras[batch_stage_idx_str] and stage != nigh_stage:
                task_to_do = pull_task(current_job_pool, stage, w)
                print('next task is ', task_to_do)
                # now check if this worker can do this task
                eligible, start_time = w.free_to_next_task(task_to_do)
                w.update_free_time(start_time = start_time, task = task_to_do)
                print(eligible, format_time(start_time))
                print(w)
                # update job next task ready time
                job_id = task_to_do.job_id
                allJobs[job_id].move_to_next_task(start_time)
                print(allJobs[job_id])
                next_task = allJobs[job_id].current_task()
                if next_task.client_idx == paras[batch_stage_idx_str]:
                    # next task is a batch task, call batch worker
                    priority = next_task.priority
                    ready = next_task.ready_time
                    # a 2 hour task
                    if priority == paras[batch_stage_idx_str]:
                        # first see if we can get a mid day batch woker
                        if ready < stage_workers[paras[batch_stage_idx_str]].start_time:








def assign_for_shift(day_index=0, period=2):
    # now i have workers for each stage, how do i assign them?
    # keep look at the first unfinished task in each job
    # find the smallest arrival task and assign to the next avaliable worker until it is impossible to assign
    ends_time = int(shift_patterns[period].end * seconds_per_hour + day_index * day_in_seconds)
    have_task_to_do = True
    # when to stop assigning? 1 no more tasks, two no worker for that stage
    while have_task_to_do:
        task_to_do = allJobs.next_task_for_each_stage(ends_time)
        if len(task_to_do) > 0:
            have_task_to_do = True
        else:
            have_task_to_do = False
        find_a_worker = False
        for stage, tasks in task_to_do.items():

            if stage != paras[batch_stage_idx_str]:
                # assign this task to next avaliable worker and mark this task finished
                # print('ready time,', tasks.task.get_ready_time())
                # print(f'perform on this task for job {tasks.job_key}')
                # print(tasks.task)
                first_ready_time = tasks.task.get_first_task_ready_time()
                worker, task_start_time = stage_workers[tasks.task.client_idx].insert_into_idle(tasks.task.get_ready_time(),
                                                                                          tasks.task.duration)
                #worker = stage_workers[stage].next_avaliable_worker(tasks.task.get_ready_time(), tasks.task.duration)
                # print('next avaliable worker')
                # print(worker)
                if worker is not None:
                    find_a_worker = True
                    # assign this task to thie worker

                    # print('after assign')
                    # print(worker)
                    # mark this task finished for that job set the task interval
                    allJobs.mark_job_task_finish(tasks.job_key, task_start_time)
                    # allJobs.show_job(tasks.job_key)
            else:
                for next_task in tasks:
                    # get priority
                    # print(f'perform on this task for job {next_task.job_key}')
                    # print(next_task.task)
                    priority = next_task.task.priority
                    if priority == paras[nine_hour_priority_idx_str]:
                        # allJobs.show_job(next_task.job_key)
                        # put this into night batch
                        # print(stage_workers[nigh_stage])
                        duration = paras['duration_2'][9]
                        worker = stage_workers[nigh_stage].next_avaliable_worker(next_task.task.get_ready_time(),
                                                                                 duration)
                        ready_time = format_time(next_task.task.get_ready_time())
                        # print(worker, ready_time, next_task.task.duration)

                        if worker is not None:
                            paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                        # print('next avaliable worker')
                        # print(worker)
                    else:
                        duration = paras['duration_2'][2]
                        # 2 hour prioity, put it into lunch batch if possible
                        # get next avalaible worker from two hour batch
                        worker = stage_workers[stage].next_avaliable_worker(next_task.task.get_ready_time(),
                                                                            duration)

                        if worker is None:
                            # passed lunch batch
                            duration = paras['duration_2'][9]
                            worker = stage_workers[nigh_stage].next_avaliable_worker(next_task.task.get_ready_time(),
                                                                                     duration)

                            if worker is not None:
                                paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                        else:
                            paras['lunch_used_embeddings'] = paras['lunch_used_embeddings'] + 1

                    if worker is not None:
                        find_a_worker = True
                        # print(next_task.task)
                        task_start_time = custom_max(worker.get_avaliable_time(), next_task.task.get_ready_time())
                        # need to update embedding duration based on lunch or night
                        next_task.task.duration = duration
                        worker.update_last_task_finish_time(next_task.task.get_ready_time(), duration)
                        allJobs.mark_job_task_finish(next_task.job_key, task_start_time)
                    # allJobs.show_job(next_task.job_key)
            # no worker is avaliable for any current task to do, a worker is avliable if next avaliable time is before shift ending time
        if find_a_worker == False:
            have_task_to_do = False

    # scheduling for this period is finsihed, get utlisation
    if day_index >= 1 and day_index <= 3:

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

def define_batch_workers(day_index):
    # for each day we have two batches slots
    # if shift starts befor mid day, define mid day batch

    period = 0
    capacity = paras['staffing'][period]
    stage = paras[batch_stage_idx_str]
    starts_time = paras['start_emdbedding'][2] + day_index * day_in_seconds
    ends_time = starts_time + paras['duration_2'][2]
    mid_day_time = 12 * seconds_per_hour + day * day_in_seconds
    lunch_start_time = 0
    lunch_end_time = 0
    paras[workers_str][day_index, period, stage] = WokrerCollection(day_index, period, stage,
                                                                        capacity[stage],
                                                                        starts_time, ends_time, mid_day_time,
                                                                        lunch_start_time,
                                                                        lunch_end_time)

    stage_workers[stage] = WokrerCollection(day_index, period, stage, capacity[stage],
                                                                        starts_time, ends_time, mid_day_time,
                                                                        lunch_start_time,
                                                                        lunch_end_time)  # print(stage_workers[stage])


    starts_time = paras['start_emdbedding'][9] + day_index * day_in_seconds
    ends_time = starts_time + paras['duration_2'][9]

    paras[workers_str][day_index, period, nigh_stage] = WokrerCollection(day_index, period, nigh_stage,
                                                                         capacity[stage],
                                                                         starts_time, ends_time, mid_day_time,
                                                                         lunch_start_time, lunch_end_time)

    stage_workers[nigh_stage] = paras[workers_str][day_index, period, nigh_stage]  # print(stage_workers[stage])


# now try to assign jobs to workers at each stage
def assign_model(current_staffing, day_index_local=1):
    paras['staffing'] = current_staffing
    paras['result'] = []
    paras['utilisation'] = []
    current_day = 17 + day_index_local

    data_rolling_window_lower_bound = day_index_local - 1
    data_rolling_window_upper_bound = day_index_local + 3

    paras[workers_str] = {}  # key day, period, stage, value is a collection of workers for that shift
    current_job_pool = []
    wait_list = []

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

            # print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            # if (len(job_data) == 0): continue
            define_jobs(current_job_pool)
            define_workers(day, idx)
            pull_job(current_job_pool)
            return
            # get_break_stats()

            # print('todao job is ', job_today)
        # print('night used', paras['night_used_embeddings'])
        # print('lunch used', paras['lunch_used_embeddings'])
    # get_gantt()
    average_time, reach_target = record_result(current_day)
    return average_time, reach_target


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