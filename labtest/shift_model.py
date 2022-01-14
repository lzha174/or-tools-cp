# solve a day's work by two parts, first part is 8am to 2pm,
# read newly arriaved jobs between this period
# plus left over jobs
# each task beocme optional, try to fit as much as possible
# a optional task can only be conducted if its previous task is performed as well
# at 2pm, scheduling reamining job/task until today's ending time, except embedding goes to 2am
#


from ortools.sat.python import cp_model
from commonstr import *
import collections
import pandas as pd
from drawing import draw


def shift_model(paras, job_data, day_index=0, period = 0):
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

    # start and end time is based on shift patterns now
    starts_time = shift_patterns[period].start * seconds_per_hour + day_index  * day_in_seconds
    ends_time =  shift_patterns[period].end * seconds_per_hour + day_index  * day_in_seconds
    format_start = format_time(starts_time)
    format_end = format_time(ends_time)
    # if shift start before 12am, if so, no need to sceduling 9hour embedding at this tage
    isBeforeMidDayBatch = shift_patterns[period].start * seconds_per_hour <= 12 * seconds_per_hour
    capacity = staffing[period]
    if isBeforeMidDayBatch:
        capacity[2] = capacity[2] - paras['lunch_used_embeddings']
    else:
        capacity[2] = capacity[2] - paras['night_used_embeddings']

    print(f'starting times are {starts_time}' + ' ' + format_start)
    print(f'ending times are {ends_time}'+ ' ' + format_end)
    print(f'capacity is {capacity}')


    model = cp_model.CpModel()



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
    l_presences = {}

    first_task_ready_times = {}
    last_added_idx = {}
    for case, tasks in job_data.items():
        previous_end = None
        previous_l = None
        j = case
        for idx, task in enumerate(tasks):
            stage = task.client_idx
            client = paras[idx_to_name_client_str][stage]
            if client in paras['95_quantile']:
                minDuration = 1
                maxDuration = int(paras['95_quantile'][client]) + 1
                horizon = ends_time
            else:
                minDuration = 7200
                maxDuration = 32400
                horizon = ends_time + maxDuration
            #print(f'horizon {horizon}')
            priority = task.priority_idx
            duration_time = task.duration
            ready_time = task.ready_time
            # note: for now i will ignore capacity used for embedding as it has a lot. i can remember them reduce capacity tho coz they happen at the same time

            if (stage == paras[batch_stage_idx_str]):
                if priority == paras[nine_hour_priority_idx_str] and isBeforeMidDayBatch:
                    # evening job cannot happen at lunch
                    # no need to scheduling this embedding in the morning period
                    last_end[j] = previous_end
                    break

            last_added_idx[j] = idx
            suffix = f'job {j} task {stage}'

            start = model.NewIntVar(starts_time, horizon, 'start ' + suffix)

            duration = model.NewIntVar(minDuration, maxDuration, 'duration' + suffix)

            end = model.NewIntVar(starts_time, horizon, 'end ' + suffix)

            l_presence = model.NewBoolVar('present '+ suffix)

            start_job[j, idx] = start
            end_job[j, idx] = end
            durations[j, idx] = duration
            l_presences[j,idx] = l_presence

            # first task start time >= ready time
            if idx == 0:
                model.Add(start >= ready_time).OnlyEnforceIf(l_presence)
                first_start[j] = start
                # remember the initial ready time
                first_task_ready_times[j] = task.first_task_ready_time

            # get last end of this job
            if idx == len(tasks) - 1:
                last_end[j] = end

            # Add precedence with previous task in the same job.
            if previous_end is not None:
                model.Add(start >= previous_end).OnlyEnforceIf(l_presence)
                # this task can only happen if previous task is performed
                model.Add(l_presence <= previous_l)

            previous_end = end
            previous_l = l_presence
            #model.Add(start + duration == end).OnlyEnforceIf(l_presence)


            task_interval = model.NewOptionalIntervalVar(start, duration, end, l_presence, 'interval' +  suffix)

            # put the interval into correct stage, a job can have duplicate tasks at the same stage such as signing out
            if stage not in stage_tasks:
                stage_tasks[stage] = [task_interval]
            else:
                stage_tasks[stage].append(task_interval)

            # add constraint for duration for embedding
            if stage == paras[batch_stage_idx_str]:

                if isBeforeMidDayBatch:
                    model.Add(duration == paras['duration_2'][two_hour_idx]).OnlyEnforceIf(l_presence)
                    # starting time is at noon

                    start_batch_time = paras['start_emdbedding'][two_hour_idx] +  day_index * day_in_seconds
                    #print(f'start lunch time is  {start_batch_time}')
                    #print('duration is ',paras['duration_2'][two_hour_idx] )
                else:
                    # evening batch
                    model.Add(duration == paras['duration_2'][six_hour_idx]).OnlyEnforceIf(l_presence)
                    start_batch_time = paras['start_emdbedding'][six_hour_idx] +  day_index * day_in_seconds
                    #print(f'start evening time is  {start_batch_time}')
                    #print('duration is ', paras['duration_2'][six_hour_idx])
                    #model.Add(l_presence == 1)

                model.Add(start == start_batch_time).OnlyEnforceIf(l_presence)


            else:
                # for all non embedding tasks, follow the duration int he data for now
                model.Add(duration == duration_time)
                # for all tasks except embedding, add constraint that end time before shift over at 6pm, start time after 8am for every task on the same day

                #model.Add(start >= starts_time).OnlyEnforceIf(l_presence)
                #model.Add(end <= ends_time).OnlyEnforceIf(l_presence)


    # add capacity constraint for stage 0
    # here the machine capacity 1 means for each machine, it can only perform one job at the same time
    # for stage 2, we have 1000 machines
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
    #objective_choice = 'max_nb_jobs'
    if objective_choice == 'max_nb_jobs':
        model.Maximize(sum(l_p * paras[job_weights_str][key[0]] for key, l_p in l_presences.items()))
    if objective_choice == 'minimise_total_case_duration':
        max_durations = {}
        case_durations = []
        for case, tasks in job_data.items():
            j = case
            max_durations[j] = []
            if j not in last_added_idx: continue
            for idx in range(last_added_idx[j] + 1):
                # i want to maximise rewards,
                # if end time -  first start time is small ,reward is big
                # how to make reward big when end time - first start time is small, means finis early
                # 1/100, 1/500 some kind of inverse function
                reward_for_this_task = model.NewIntVar(0, day_in_seconds * 4, 'total duration {} {}'.format(j, idx))
                model.Add(reward_for_this_task== day_in_seconds * 4- (end_job[j, idx] - first_task_ready_times[j])).OnlyEnforceIf(l_presences[j, idx])
                model.Add(reward_for_this_task == 0).OnlyEnforceIf(l_presences[j, idx].Not())
                case_durations.append(reward_for_this_task)

        model.Maximize(sum(case for case in case_durations))
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
        finished_count = 0
        for case, tasks in job_data.items():
            j = case
            floatted_flag = None
            for idx, task in enumerate(tasks):
                t = task.client_idx
                task_name = paras[idx_to_name_client_str][t]
                l_presence = l_presences.get((j, idx), None)
                if l_presence  is None or solver.Value(l_presence) == False:
                    # this task is the first unfinished task
                    # floatted_day = solver.Value(start) // day_in_seconds
                    floatted_day = day_index + 1
                    #print(f'folloated_day is  {floatted_day}')
                    # the ready_time is the initial ready time for the very first task,
                    # this is useful cos we need some kind of memoerizing its actual duration for decomposing
                    # this ready time will help us define reward properly
                    if idx > 0:
                        ready_time = solver.Value(end_job[j, idx - 1])
                    else:
                        ready_time = task.ready_time

                    unfinished = unfinshed_job_type(case_idx=j, task_idx=idx,
                                                        ready_time=ready_time)
                    if floatted_day not in unfinished_jobs:
                        # remember the first unifished task
                        unfinished_jobs[floatted_day] = [unfinished]
                    else:
                        unfinished_jobs[floatted_day].append(unfinished)
                    floatted_flag = j
                    break
                # else this task is finished

                if idx == len(tasks) - 1:
                    finished_count = finished_count + 1

                start_time = format_time(solver.Value(start_job[j, idx]))
                end_time = format_time(solver.Value(end_job[j, idx]))

                formatted_start_time[j, idx] = start_time
                formatted_end_time[j, idx] = end_time
                duration = solver.Value(durations[j, idx])
                formatted_durations[j, idx] = duration
                case_key = paras[idx_to_name_key_str][j]
                # the ready time should the today's morning time
                fomratted_ready = format_time(task.ready_time)

                # if ((t != paras[batch_stage_idx_str]) and solver.Value(end_job[j, idx]) <= ends_time[0])\
                #        or ( t == paras[batch_stage_idx_str] and solver.Value(end_job[j, idx]) <= starts_time[1]) :
                if True:
                    # this task is finished today
                    new_ready_time = fomratted_ready
                    if idx > 0:
                        new_ready_time = format_time(solver.Value(end_job[j, idx - 1]))
                    case_name = paras[idx_to_name_key_str][case]
                    finished_data = [case, case_name, task_name, task.order, start_time, end_time, duration, new_ready_time]
                    l_str = 'job {} task {} start {}, duration {}, end {} ready {}\n'.format(case, task_name,
                                                                                                 start_time, duration,
                                                                                                 end_time,
                                                                                                 new_ready_time)
                    if t==paras[batch_stage_idx_str]:
                        # need to remove used embedding for today
                        if solver.Value(durations[j, idx]) == maxDuration:
                            paras['night_used_embeddings'] = paras['night_used_embeddings'] + 1
                        else:
                            # lunchy embedding
                            paras['lunch_used_embeddings'] = paras['night_used_embeddings'] + 1
                    logstr.append(l_str)
                    paras['result'].append(finished_data)
                    # a batch task finish after mid nite is also considered finished
                    if j == 7:
                        print(f'job {case} task {task_name}')

                        print(
                        'start {}, duration {}, end {} ready {}'.format(start_time, duration,
                                                                       end_time, first_task_ready_times[j]))




        new_jobs = {}  # construst floatted job for next day
        #print(f'day_index is {day_index}')
        key = day_index + 1
        if key in unfinished_jobs:
            jobs = unfinished_jobs[key]

            #print(f'day {key} unfinished {unfinished_jobs[key]}')
            # print('unifished {}'.format(len(unfinished_jobs[key])))
            for job in jobs:
                job_key = job.case_idx
                task_key = job.task_idx
                ready_time = job.ready_time

                original_tasks = job_data[job_key]
                unfinished_tasks = original_tasks[task_key:]
                # change first unifinished task ready time to be last finished job end time
                new_tasks = []
                for idx, task in enumerate(unfinished_tasks):
                    if idx == 0:
                        task = task._replace(ready_time=ready_time)
                    new_tasks.append(task)
                    #print('ready_time', format_time(task.ready_time))

                new_jobs[job_key] = new_tasks

                #for key, jobs in new_jobs.items():
                 #   print(f'new jobs {key} are {jobs}')
                paras['unfinished'] = new_jobs

        # this is useful to calculate performace

        # paras['result'].append(df)

        if status == cp_model.FEASIBLE:
            print('Fesible')
        else:
            print('Optimal')

        write_to_file(logstr)
        print(f'finish {finished_count}')