import copy

from schedulingclasses import *
from utilisation_plot import *

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
    x = paras['day_jobs'].get((day, period), None)
    if x is None: return
    if row.case_key not in paras['day_jobs'][day, period]: return

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
        bench_ready_time[case_key_idx] = min(row.work_ready_timestamp, bench_ready_time[case_key_idx])
        bench_finish_time[case_key_idx] = max(row.end_timestamp, bench_finish_time[case_key_idx])
        min_ready_times[case_key_idx] = min(row.ready_time_sec, min_ready_times[case_key_idx])

    if case_key_idx not in case_max_stages:
        case_max_stages[case_key_idx] = case_stage_rank
    else:
        case_max_stages[case_key_idx] = max(case_max_stages[case_key_idx], case_stage_rank)
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


def read():
    df = load_real_data()
    return df


def load_new_day(df, day, period):
    df.apply(row_process, args=(day, period,), axis=1)
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


df = read()


def add_finished_stats(row, finished_case):
    case_key_index = row.case_key
    case_stage_rank = row.case_stage_rank
    if case_max_stages[case_key_index] == case_stage_rank:
        finished_case.append(case_key_index)



def record_result(start_date=18):
    logstr = []
    end_date = start_date + 1
    result_df = pd.DataFrame(paras['result'],
                             columns=['case_key', 'case_name', 'client', 'case_stage_rank', 'start', 'end',
                                      'duration', 'ready_time'])
    result_df[["start", "end", "ready_time"]] = result_df[["start", "end", "ready_time"]].apply(pd.to_datetime)
    result_df = result_df.sort_values(["case_key", "case_stage_rank"], ascending=(False, True))
    # result_df.to_csv('out.csv', index=False)
    to_csv(result_df, 'out.csv')
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
    tmp = stats_df[stats_df['client'] == 'Signout']
    tmp.apply(add_finished_stats, args=(finished_case,), axis=1)
    # get case_key

    print('finished case', len(finished_case))
    finished = stats_df[stats_df['case_key'].isin(finished_case)]
    # finished.to_csv('stats_out.csv', index=False)
    to_csv(finished, 'stats_out.csv')

    # the last stage is signout

    ready_time_df = finished.groupby(by='case_key').ready_time.min()
    max_end_time_df = finished.groupby(by='case_key').end.max()

    # print(max_end_time_df, ready_time_df)
    # print(max_end_time_df - ready_time_df)

    total_duration_df = pd.DataFrame(max_end_time_df - ready_time_df, columns=['optimised_duration'])
    print('or average', total_duration_df['optimised_duration'].mean())
    logstr.append('average {} \n'.format(total_duration_df['optimised_duration'].mean()))
    staffingconfig = []
    for key, value in staffing[0, 2].items():
        s = f'{key} : {value} \n'
        staffingconfig.append(s)
    staffingconfig.append('average {} \n'.format(total_duration_df['optimised_duration'].mean()))
    # write_to_file('log.txt', staffingconfig)

    bench_duration = {}
    for key in finished_case:
        bench_duration[key] = bench_finish_time[key] - bench_ready_time[key]
    bench_duration_df = pd.DataFrame.from_dict(bench_duration, orient='index', columns=['bench_duration'])
    print('bench mean value', bench_duration_df.mean())

    result = pd.concat([total_duration_df, bench_duration_df], axis=1, join='inner')
    # result.to_csv('duration_compare.csv')
    to_csv(result, 'duration_compare.csv')

    utilisation_df = pd.DataFrame(paras['utilisation'],
                                  columns=['day', 'period', 'client', 'utilisation'])
    # utilisation_df.to_csv('utilisation.csv', index=False)
    to_csv(utilisation_df, 'utilisation.csv')
    for day in range(start_date, end_date):
        stats_start_ready_date = f'2021-05-{day} 00:00:00'

        stat_end_ready_date = f'2021-05-{day + 1} 00:00:00'

        f = df[df.case_stage_rank == 1]
        f = f[(f.work_ready_timestamp > stats_start_ready_date) & (f.work_ready_timestamp < stat_end_ready_date)]
        print(stats_start_ready_date, len(f.index))

        f_finished = stats_df[stats_df.case_stage_rank == 1]
        f_finished = f_finished[
            (f_finished.min_ready_time > stats_start_ready_date) & (f_finished.min_ready_time < stat_end_ready_date)]
        print(stats_start_ready_date + ' finished ', len(f_finished.index))
        assert (len(finished_case) == len(f_finished))

    return total_duration_df['optimised_duration'].mean()


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


def get_gantt():
    selected_day = 1
    selected_period = 1
    selected_stage = 1
    selected_worker = 1

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
    to_csv(result_df, 'worker_gantt.csv')

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
    draw_gannt(output)


def define_workers(day_index=0, period=2):
    # define workers for each stage
    global stage_workers
    capacity = paras['staffing'][day_index, period]

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
            starts_time = paras['start_emdbedding'][two_hour_idx] + day_index * day_in_seconds
            ends_time = starts_time + 2 * seconds_per_hour
            paras[workers_str][day_index, period, stage] = WokrerCollection(day_index, period, stage,
                                                                            capacity[stage] - capacity_used,
                                                                            starts_time, ends_time, lunch_start_time,
                                                                            lunch_end_time)

            stage_workers[stage] = paras[workers_str][day_index, period, stage]  # print(stage_workers[stage])

            capacity_used = paras['night_used_embeddings']
            starts_time = paras['start_emdbedding'][nine_hour_idx] + day_index * day_in_seconds
            ends_time = starts_time + 9 * seconds_per_hour

            paras[workers_str][day_index, period, nigh_stage] = WokrerCollection(day_index, period, nigh_stage,
                                                                                 capacity[stage] - capacity_used,
                                                                                 starts_time, ends_time,
                                                                                 lunch_start_time, lunch_end_time)

            stage_workers[nigh_stage] = paras[workers_str][day_index, period, nigh_stage]  # print(stage_workers[stage])

            # for embedding we need two stages, one for lunch one for night


def define_jobs():
    global job_data
    global allJobs

    for job, tasks in job_data.items():
        new_job = JobClass()
        new_job.set_job(job, tasks)
        # print(new_job)
        allJobs.add_job(new_job)
    # allJobs.next_task_stage(0)


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
                job_key = tasks.job_key

                # print(tasks.task)
                first_ready_time = tasks.task.get_first_task_ready_time()
                worker = stage_workers[stage].next_avaliable_worker(tasks.task.get_ready_time(), tasks.task.duration)
                # print('next avaliable worker')
                # print(worker)
                if worker is not None:
                    find_a_worker = True
                    # assign this task to thie worker
                    task_start_time = max(worker.get_avaliable_time(), tasks.task.get_ready_time())
                    worker.update_avaliable_time(tasks.task.get_ready_time(), tasks.task.duration)
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
                        worker = stage_workers[nigh_stage].next_avaliable_worker(next_task.task.get_ready_time(),
                                                                                 next_task.task.duration)
                        ready_time = format_time(next_task.task.get_ready_time())
                        # print(worker, ready_time, next_task.task.duration)
                        duration = 9 * seconds_per_hour
                        if worker is not None:
                            paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                        # print('next avaliable worker')
                        # print(worker)
                    else:
                        # 2 hour prioity, put it into lunch batch if possible
                        # get next avalaible worker from two hour batch
                        worker = stage_workers[stage].next_avaliable_worker(next_task.task.get_ready_time(),
                                                                            next_task.task.duration)
                        duration = 2 * seconds_per_hour
                        if worker is None:
                            # passed lunch batch
                            worker = stage_workers[nigh_stage].next_avaliable_worker(next_task.task.get_ready_time(),
                                                                                     next_task.task.duration)
                            duration = 9 * seconds_per_hour
                            if worker is not None:
                                paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                        else:
                            paras['lunch_used_embeddings'] = paras['lunch_used_embeddings'] + 1

                    if worker is not None:
                        find_a_worker = True
                        # print(next_task.task)
                        task_start_time = max(worker.get_avaliable_time(), next_task.task.get_ready_time())
                        worker.update_avaliable_time(next_task.task.get_ready_time(), duration)
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


# now try to assign jobs to workers at each stage
def assign_model(current_staffing, day_index_local=1):
    paras['staffing'] = current_staffing
    paras['result'] = []
    paras['utilisation'] = []
    current_day = 17 + day_index_local

    paras[workers_str] = {}  # key day, period, stage, value is a collection of workers for that shift

    global job_data
    for day, data_windows in day_data_windows.items():
        job_today = 0
        paras['night_used_embeddings'] = 0
        paras['lunch_used_embeddings'] = 0
        # index of data_window is same as shift_patterns
        for idx, data_window in enumerate(data_windows):
            job_data = {}
            paras['full'] = False
            load_new_day(df, day, idx)
            job_today = job_today + len(job_data)
            # print('total jobs {} for day {} period {} is {}'.format(data_window, day, idx, len(job_data)))
            # if (len(job_data) == 0): continue
            define_jobs()
            define_workers(day, idx)
            assign_for_shift(day, idx)
            #get_break_stats()

            # print('todao job is ', job_today)
        # print('night used', paras['night_used_embeddings'])
        # print('lunch used', paras['lunch_used_embeddings'])
    get_gantt()
    average_time = record_result(current_day)
    return average_time


# assign_model(staffing)

# i want to define a simple local search for a day's shift? fix all other day's shift
def shift_local_search():
    paras['result'] = []
    paras['utilisation'] = []
    paras['staffing'] = None
    nb_staff_to_add = 5

    logstr = []
    day = 1

    while day <= 2:
        print('im at day ', day)
        current_average = assign_model(staffing, day)
        print(f'current average {current_average}')
        logstr.append(f'current average before adding staff for day {day} is {current_average}')
        for i in range(nb_staff_to_add):
            outstr = f'add staff {i}\n'
            logstr.append(outstr)
            # say i want to add one more staff, i want to know add to which stage
            stage_to_search = [0, 1, 3, 4]
            # improvement = {}

            # add 1 worker to the best shift, in each shift , find the best stage
            max_improvment_shift = -1e15
            best_shift = None
            best_average_shift = None
            best_shift_stage = None
            for shift_period in range(max_shift_key + 1):
                logstr.append(f'go through shift {shift_period}\n')

                max_improvment_this_shift = -1e15
                best_average_this_shift = None
                best_stage_this_shift = None
                # given a shift period, find the best stage to add a worker

                for s in stage_to_search:
                    temp_staffing = copy.deepcopy(staffing)

                    temp_staffing[day, shift_period][s] = temp_staffing[day, shift_period][s] + 1
                    # print('current staffing', temp_staffing)
                    new_average = assign_model(temp_staffing, day)
                    improvement = current_average - new_average
                    to_seconds = improvement.total_seconds()
                    print('to seconds', to_seconds)
                    if to_seconds > 0 and to_seconds > max_improvment_this_shift:
                        max_improvment_this_shift = to_seconds
                        best_stage_this_shift = s
                        best_average_this_shift = new_average

                if max_improvment_this_shift > max_improvment_shift:
                    max_improvment_shift = max_improvment_this_shift
                    best_shift = shift_period
                    best_average_shift = best_average_this_shift
                    best_shift_stage = best_stage_this_shift

            if best_shift is not None:
                logstr.append(f'best shift is {best_shift} best stage is {best_shift_stage}\n')
                logstr.append(f'best improvment is {best_average_shift}\n')
                staffing[day, best_shift][best_shift_stage] = staffing[day, best_shift][best_shift_stage] + 1
                current_average = best_average_shift
                print(current_average)
            else:
                # no need to continue adding
                break
            for shift_period in range(max_shift_key + 1):
                for s in stage_to_search:
                    logstr.append(
                        f'day {day} period {shift_period} stage {s} staff are {staffing[day, shift_period][s]}\n')
        day = day + 1
    write_to_file('log.txt', logstr)


def repeat_local_search():
    counter = 0
    while counter < 2:
        shift_local_search()
        counter = counter + 1


# repeat_local_search()
assign_model(staffing, 1)
