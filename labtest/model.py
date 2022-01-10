
from ortools.sat.python import cp_model
from commonstr import *
import collections
import pandas as pd
from drawing import draw
def lab_model(paras, job_data, day_index=0):
    # every job has 4 tasks, third task is processed in batch, capacity is 1000
    # every task of job start time and end time for stage 0, 1, 3 must fall in the normal working hours 8am - 6 pm  every day
    # every task for stage 2 must start at 12pm or 6pm, first start lasts 2 hours, second start lasts 9 hours
    # a job has category 0 can start stage 2 at both time slots, with category 1 can only start at evening slot 6pm

    # define starting time, ending time in mins for 3 days
    # this need to be changed
    logstr = []
    
    maxDuration = 9 * seconds_per_hour


    two_hour_idx = 0
    six_hour_idx = 1

    days = range(paras['days'])
    starts_time = []
    ends_time = []
    # this is correct

    for d in days:
        start = paras['start'] * seconds_per_hour + (day_index + d) * day_in_seconds
        end = paras['end'] * seconds_per_hour + (day_index + d) * day_in_seconds
        starts_time.append(start)
        ends_time.append(end)
    print(f'starting times are {starts_time}')
    print(f'ending times are {ends_time}')

    model = cp_model.CpModel()
    horizon = ends_time[-1]
    print(f'horizon {horizon}')

    # define task intervals for each job

    # starting time of tasks of each job
    start_job = {}  # index by job_id, stage id
    # ending time of tasks of each job
    end_job = {}
    durations = {}
    # this needs to be chagnes
    stage_tasks = {}  # indexed by stage id
    first_start = {}
    last_end = {}

    ready_times = {}
    for case, tasks in job_data.items():
        previous_end = None
        j = case
        for idx, task in enumerate(tasks):
            stage = task.client_idx
            client = paras[idx_to_name_client_str][stage]
            if client in paras['95_quantile']:
                minDuration = 1
                maxDuration = int(paras['95_quantile'][client]) + 1
            else:
                minDuration = 7200
                maxDuration = 32400
            priority = task.priority_idx
            duration_time = task.duration
            ready_time = task.ready_time

            suffix = f'job {j} task {stage}'

            start = model.NewIntVar(starts_time[0], horizon, 'start ' + suffix)

            duration = model.NewIntVar(minDuration, maxDuration, 'duration' + suffix)

            end = model.NewIntVar(starts_time[0], horizon, 'end ' + suffix)

            start_job[j, idx] = start
            end_job[j, idx] = end
            durations[j, idx] = duration

            # first task start time >= ready time
            if idx == 0:
                model.Add(start >= ready_time)
                first_start[j] = start
                # remember the initial ready time
                ready_times[j] = ready_time

            # get last end of this job
            if idx == len(tasks) - 1:
                last_end[j] = end

            # Add precedence with previous task in the same job.
            if previous_end is not None:
                model.Add(start >= previous_end)
            previous_end = end
            model.Add(start  + duration == end)
            task_interval = model.NewIntervalVar(start, duration, end, 'interval ' + suffix)

            # put the interval into correct stage, a job can have duplicate tasks at the same stage such as signing out
            if stage not in stage_tasks:
                stage_tasks[stage] = [task_interval]
            else:
                stage_tasks[stage].append(task_interval)

            # add constraint for duration for embedding
            if stage == paras[batch_stage_idx_str]:

                l_lunch_batch = model.NewBoolVar('lunch ' + suffix)

                # starting time is evening  -- l_lunch_batch == False
                # evening batch take 6 hours
                model.Add(duration == paras['duration_2'][six_hour_idx]).OnlyEnforceIf(l_lunch_batch.Not())
                l_stage_2_evening_start = []
                for d in days:
                    # cannot start evening batch after the last day midnite
                    if d == paras['days'] - 1: continue
                    # if this task start at day d, make sure it start at 12pm
                    start_time_evening = paras['start_emdbedding'][six_hour_idx] + (d + day_index) * day_in_seconds
                    l_stage_2_evening_task_start = model.NewBoolVar('stage 2 evening in {} {}'.format(d, suffix))
                    model.Add(start == start_time_evening).OnlyEnforceIf(
                        [l_stage_2_evening_task_start, l_lunch_batch.Not()])
                    l_stage_2_evening_start.append(l_stage_2_evening_task_start)
                # if we have this job at evening, one of the evening start day must be chosen
                model.Add(sum(s for s in l_stage_2_evening_start) == 1).OnlyEnforceIf(l_lunch_batch.Not())

                if priority == paras[two_hour_priority_idx_str]:
                    # this task can start at both batch

                    # lunch batch take 2 hours
                    model.Add(duration == paras['duration_2'][two_hour_idx]).OnlyEnforceIf(l_lunch_batch)
                    # starting time is at noon
                    l_stage_2_lunch_start = []
                    for d in days:
                        # if this task start at day d, make sure it start at 12pm
                        start_time_lunch = paras['start_emdbedding'][two_hour_idx] + (d + day_index) * day_in_seconds
                        l_stage_2_lunch_task_start = model.NewBoolVar('stage 2 lunch in {} {}'.format(d, suffix))
                        model.Add(start == start_time_lunch).OnlyEnforceIf([l_stage_2_lunch_task_start, l_lunch_batch])
                        l_stage_2_lunch_start.append(l_stage_2_lunch_task_start)
                    # if we have this job at lunch, one of the mid day must be chosen
                    model.Add(sum(s for s in l_stage_2_lunch_start) == 1).OnlyEnforceIf(l_lunch_batch)

                else:  # category 1 can only happen at evening batch
                    model.Add(l_lunch_batch == 0)
            else:
                # for all non embedding tasks, follow the duration int he data for now
                model.Add(duration == duration_time)
                # for all tasks except embedding, add constraint that end time before shift over at 6pm, start time after 8am for every task on the same day
                l_in_day = {}
                for d in days:
                    l_in_d = model.NewBoolVar('task of {j} in {d}')
                    model.Add(start >= starts_time[d]).OnlyEnforceIf(l_in_d)
                    model.Add(end <= ends_time[d]).OnlyEnforceIf(l_in_d)
                    l_in_day[d] = l_in_d
                # one of the days must be chosen
                model.AddBoolXOr([item for key, item in l_in_day.items()])



    # add capacity constraint for stage 0
    # here the machine capacity 1 means for each machine, it can only perform one job at the same time
    # for stage 2, we have 1000 machines
    capacity = paras['capacity']
    for stage in stage_tasks:
        # capacity is 1, means no tasks at the same stage can overlap
        # capacity is 2, means two tasks can overlap, as we have two machines
        # stage 2 has 1000 machines, ha
        model.AddCumulative(stage_tasks[stage], [1] * len(stage_tasks[stage]), capacity[stage])

    # lets start with object to be makespan

    # print(f'starts {start_job}')
    # print(f'ends {end_job}')
    # print(f'durations { durations}')
    # print(f'stage tasks {stage_tasks}')

    # Makespan objective.
    # objective_choice = 'minimise_max_end_time'
    # objective_choice = 'minimise_max_case_duration'
    objective_choice = 'minimise_total_case_duration'

    # try minimise total in system time
    if objective_choice == 'minimise_total_case_duration':

        # this needs to change


        # wieghted duration - floated job has more weights
        model.Minimize(
            sum((last_end[i] - ready_times[i]) * paras[job_weights_str][i] for i in job_data))
        # model.Minimize(sum(last_end[i]  for i in paras['allJobs']))
    # try minimise max in system time
    if objective_choice == 'minimise_max_case_duration':
        in_systems = []
        for j in range(len(job_data)):
            suffix = 'in_system_{j}'
            in_system_var = model.NewIntVar(0, horizon, suffix)
            model.Add(in_system_var == paras[job_weights_str][j] * (last_end[j] - ready_times[j]))
            in_systems.append(in_system_var)

        in_system_obj = model.NewIntVar(0, horizon, 'in_system')
        model.AddMaxEquality(in_system_obj, in_systems)
        model.Minimize(in_system_obj)
    # Solve.
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = paras['max_serach_time_sec']
    status = solver.Solve(model)
    print(status, cp_model.INFEASIBLE, cp_model.FEASIBLE)
    # Print solution.
    if status == cp_model.FEASIBLE or status == cp_model.OPTIMAL:

        # need to output all start and end time for each job at each stage

        print(status)
        case_durations = []
        formatted_start_time = {}  # indexed by job id and task id
        formatted_end_time = {}
        formatted_durations = {}
        x_pairs_dict = {}  # indexed by stage id store (start_time, duration) for all jobs

        # get all jobs that not finished today
        currentDay = 0
        currentDayEndingTime = day_in_seconds * (day_index + 1)

        # this needs to change

        # I want to store unifinished jobs based on day index, only deal with jobs that not finished tmr for next day planning
        unfinished_jobs = {}  # key = day, arrays of first unfinished stage
        unfinshed_job_type = collections.namedtuple('unfinished', 'case_idx, task_idx ready_time')
        finished_today = []
        for case, tasks in job_data.items():
            j = case
            floatted_flag = None
            for idx, task in enumerate(tasks):
                t = task.client_idx
                task_name = paras[idx_to_name_client_str][t]
                start = start_job.get((j, idx), None)
                if start is None: continue

                # a batch job may start next day evening, or lunch, view this as unfinished and rolled to next day
                if solver.Value(start) >= starts_time[1] and floatted_flag is None:
                    # as long as a non batch task did not finish today , we put the remaining tasks into next day planning

                    #floatted_day = solver.Value(start) // day_in_seconds
                    floatted_day = day_index + 1
                    print(f'folloated_day is  {floatted_day}')
                    if idx > 0:
                        unfinished = unfinshed_job_type(case_idx=j, task_idx= idx, ready_time= solver.Value(end_job[j, idx-1]))
                    else:
                        unfinished = unfinshed_job_type(case_idx=j, task_idx=idx,
                                                        ready_time=task.ready_time)
                    if floatted_day not in unfinished_jobs:
                        # remember the first unifished task
                        unfinished_jobs[floatted_day] = [unfinished]
                    else:
                        unfinished_jobs[floatted_day].append(unfinished)
                    floatted_flag = j


                start_time = format_time(solver.Value(start_job[j, idx]))
                end_time = format_time(solver.Value(end_job[j, idx]))

                formatted_start_time[j, idx] = start_time
                formatted_end_time[j, idx] = end_time
                duration = solver.Value(durations[j, idx])
                formatted_durations[j, idx] = duration
                case_key = paras[idx_to_name_key_str][j]
                # the ready time should the today's morning time
                fomratted_ready = format_time(task.ready_time)



                #if ((t != paras[batch_stage_idx_str]) and solver.Value(end_job[j, idx]) <= ends_time[0])\
                #        or ( t == paras[batch_stage_idx_str] and solver.Value(end_job[j, idx]) <= starts_time[1]) :
                if True:
                    # this task is finished today
                    new_ready_time = fomratted_ready
                    if idx > 0:
                        new_ready_time = format_time(solver.Value(end_job[j, idx-1]))

                    finished_data = [case, task_name, task.order, start_time, end_time, duration, new_ready_time]
                    if j == 7:
                        l_str = 'job {} task {} start {}, duration {}, end {} ready {}\n'.format(case, task_name,
                                                                                                 start_time, duration,
                                                                                                 end_time,
                                                                                                 new_ready_time)
                        logstr.append(l_str)
                    paras['result'].append(finished_data)
                # a batch task finish after mid nite is also considered finished



                    
                    print(f'job {case} task {task_name}')

                    print(
                        'start {}, duration {}, end {} ready {}'.format(start_time, duration,
                                                              end_time, ready_times[j]))

                if t not in x_pairs_dict:
                    x_pairs_dict[t] = [(solver.Value(start_job[j, idx]), duration)]
                else:
                    x_pairs_dict[t].append((solver.Value(start_job[j, idx]), duration))

                if floatted_flag is not None:
                    job_duration = (solver.Value(last_end[j]) - ready_times[j]) / day_in_seconds

                    # todo case duration is not correct in this veroion
                    case_durations.append(job_duration)
            if floatted_flag is None:
                finished_today.append(j)
        new_jobs = {} # construst floatted job for next day
        print(f'day_index is {day_index}')
        key = day_index + 1
        jobs = unfinished_jobs[key]

        print(f'day {key} unfinished {unfinished_jobs[key]}')
            #print('unifished {}'.format(len(unfinished_jobs[key])))
        for job in jobs:
            job_key = job.case_idx
            task_key = job.task_idx
            ready_time = job.ready_time

            original_tasks = job_data[job_key]
            unfinished_tasks = original_tasks[task_key:]
            # change readty_time to tmr starting time
            new_tasks = []
            for idx, task in enumerate(unfinished_tasks):
                if idx == 0:
                    task = task._replace(ready_time=ready_time)
                new_tasks.append(task)
                print('ready_time', format_time(task.ready_time))

            new_jobs[job_key] = new_tasks

        for key, jobs in new_jobs.items():
            print (f'new jobs {key} are {jobs}')
        paras['unfinished'] = new_jobs

        print ('finished {}'.format(finished_today))
        print ('total jobs', len(finished_today) + len(new_jobs))

        all_results = []  # each row - job id, stage , start, end duration
        # i want to save all finished tasks today

        for case in finished_today:
            j = case
            #print(f'case is {case}')
            tasks = job_data[case]
            for idx, task in enumerate(tasks):
                t = task.client_idx
                clinet = paras[idx_to_name_client_str][t]
                start = formatted_start_time.get((j, idx), None)
                if start is None: continue

                finished_data = ['job_{i}'.format(i=case)]
                end = formatted_end_time.get((j, idx), None)
                duration = formatted_durations.get((j, idx), None)
                finished_data = finished_data + [clinet, start, end, duration]
                all_results.append(finished_data)


        # this is useful to calculate performace


        #paras['result'].append(df)



        # todo average case duration is not correct in this floatted version
        if objective_choice == 'minimise_total_case_duration':
            print('average duration {}'.format(solver.ObjectiveValue() / day_in_seconds))
        else:
            print('max in_system duration =', solver.ObjectiveValue() / day_in_seconds)
        if status == cp_model.FEASIBLE:
            print('Fesible')
        else:
            print('Optimal')

        # now crated floatted jobs
        floatted_job_ops_duration = []
        floatted_job_ready_time = []
        floatted_job_start_stage = []
        floatted_job_category = []

        key = day_index + 1

        floatted_jobs = unfinished_jobs.get(key, [])

        for stage in range(0,5):
            start = x_pairs_dict.get(stage, None)

            if start is None: continue
            x_pairs = x_pairs_dict[stage]
            day_start_times = [ (d + day_index) * day_in_seconds for d in days] + [ (d+1 + day_index) * day_in_seconds]
            draw.draw_gannt(x_pairs, stage, day_start_times, day_index)

        print(paras['result'])
        write_to_file(logstr)