import collections

from ortools.sat.python import cp_model

from drawing import draw

import pandas as pd
import openpyxl
from datetime import datetime
from datetime import timedelta

# now change to
seconds_per_hour = 60 * 60
day_in_seconds = 24 * 60 * 60

paras = {
    'result': [], # finished job performance on every day, key is day index, value is dataframe array
    'job_data':[], # contains processing time for each stage
    'job_index': [],
    'category': [],# 0 means it can be at 12-2pm or 6pm-3am batch, 1 means it only be at 6pm-3pm batch, processing time is 2 hour or 9 hours
    'ready_time': [],
    'days': 3, # planning for 3 days
    'start':8, # start time for stage 0, 1, 3 every day, 8am
    'end': 18, # end time for stage 0,1,3 every day 6pm
    'start_2': [12 * seconds_per_hour, 18 * seconds_per_hour], # start time for category 0 and 1 at stage 2, 12pm, and 6 pm
    'duration_2': [2 * seconds_per_hour , 9 * seconds_per_hour], # duration for category 0 and 1 at stage 2 in minutes
    'max_jobs': 2,
    'max_serach_time_sec': 30,
    'capacity':[10, 10, 1000, 15], # how many samples can be anaylsed at the same time for each stage
    'stage':[], # the job starting tage for this scheduling period, it can be floatted from previous planning period, only add intervals for the reamining stages
    'floatted_jobs':[],  # floatted job index
    'number_floatted': 0,
    'floatted_job_stage' : [], # floatted job next stage this will not be any stage 2 job
    'job_weight': [] # make floatted job has more weights so they finish as soon as possible

}

def clean_data():
    paras['floatted_jobs'] = []
    paras['floatted_job_stage'] = []

def check(row):
    if row['client'] == "Grossing" and row['case_stage_rank'] != 2:
        print (row.case_key)

def load_real_date():
    df = pd.read_csv('3-day.csv')
    df['start_timestamp'] = pd.to_datetime(df['start_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['end_timestamp'] = pd.to_datetime(df['end_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['work_ready_timestamp'] = pd.to_datetime(df['work_ready_timestamp'], format='%Y-%m-%d %H:%M:%S')
    # https://www.kite.com/python/answers/how-to-find-the-number-of-seconds-between-two-datetime-objects-in-python

    past_date = pd.to_datetime('17/05/2021 00:00')

    df['duration'] = df['end_timestamp'] - df["start_timestamp"]

    df['ready_time_sec'] = df['work_ready_timestamp'].apply(lambda x: (x - past_date).total_seconds())

    df['duration_sec'] = df['duration'].apply(lambda x: x.total_seconds())
    print(df['start_timestamp'])
    print(df['duration_sec'])
    print(df['ready_time_sec'])
    
    print(df.info())

    temp1 = df[df.client == "Grossing"]

    temp2 = df[df.case_stage_rank==2]
    print(temp1.info())
    print(temp2.info())

    df.apply(check, axis = 1)

def format_time(n = 15):
    # Given timestamp in string
    time_str = '01/01/2022 00:00:00'
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


def process_data(row):

    if len(paras['job_data']) - paras['number_floatted'] >= paras['max_jobs']: return

    data = [row['stage_0'] * 60, row['stage_1'] * 60, row['stage_2'] * 60, row['stage_3'] * 60]  # processing time in minutes at every stage escept stage 2, fixed time


    # this is the orignial row index in the whole df
    paras['job_index'].append(row.name)
    #
    paras['job_data'].append(data)

    paras['category'].append(row['category'])
    #paras['category'].append(0)

    paras['ready_time'].append(row['ready_time_secs']) # base time is 2022/01/01 12:00 am
    #paras['ready_time'].append(0)

    paras['stage'].append(row.stage)
    #paras['stage'].append(0)
    
    paras['job_weight'].append(1)


    return

def load_data():
    df = pd.read_csv('1500-secs.csv')
    paras['df'] = df

def read_data(day_index = 0):
    df = paras['df']
    df = df[(df['ready_time_secs'] >= day_in_seconds* day_index) & (df['ready_time_secs'] < day_in_seconds * (day_index + 1))]
    print(df.head)
    df.apply(process_data, axis = 1)

    # how to read data for a certain time range

    numTasks = 4
    allTasks = range(numTasks)
    print(len(paras['job_data']), len(paras['ready_time']))
    paras['numTasks'] = numTasks
    paras['allTasks'] = allTasks

def lab_model(day_index = 0):
    clean_data()
    # every job has 4 tasks, third task is processed in batch, capacity is 1000
    # every task of job start time and end time for stage 0, 1, 3 must fall in the normal working hours 8am - 6 pm  every day
    # every task for stage 2 must start at 12pm or 6pm, first start lasts 2 hours, second start lasts 9 hours
    # a job has category 0 can start stage 2 at both time slots, with category 1 can only start at evening slot 6pm

    # define starting time, ending time in mins for 3 days

    OpsDuration = paras['job_data']
    print('duration is ', OpsDuration)
    maxDuration = 9 * seconds_per_hour

    category = paras['category']
    two_hour_idx = 0
    six_hour_idx = 1

    days = range( paras['days'])
    starts_time = []
    ends_time = []
    for d in days:
        start = 8 * seconds_per_hour + (day_index + d) * day_in_seconds
        end = 18 * seconds_per_hour + (day_index + d) * day_in_seconds
        starts_time.append(start)
        ends_time.append(end)
    print(f'starting times are {starts_time}')
    print(f'ending times are {ends_time}')

    model = cp_model.CpModel()
    horizon = ends_time[-1]
    print(f'horizon {horizon}')

    # define task intervals for each job

    # starting time of tasks of each job
    start_job = {} # index by job_id, stage id
    # ending time of tasks of each job
    end_job = {}
    durations = {}
    jobs = {} # intervals index by job_id, task id
    stage_tasks = {} # indexed by stage id
    first_start = []
    last_end = []
    for j in range(len(paras['job_data'])):
        #start_job[j] = {}
        #end_job[j] =
        #durations[j] = []

        previous_end = None
        #print('j=', j)
        for stage in paras['allTasks']:
            # only process reamining tasks
            if stage < paras['stage'][j]: continue
            #print('stage = ', stage)
            suffix = f'job {j} task {stage}'

            start = model.NewIntVar(starts_time[0], horizon, 'start ' + suffix)

            duration = model.NewIntVar(0, maxDuration, 'duration' + suffix)

            end = model.NewIntVar(starts_time[0], horizon, 'end ' + suffix)


            start_job[j, stage] = start
            end_job[j, stage] = end
            durations[j, stage] = duration

            # first task start time >= ready time
            if stage == paras['stage'][j]:

                model.Add(start >= paras['ready_time'][j])
                first_start.append(start)

            if stage == 3:
                last_end.append(end)

            # Add precedence with previous task in the same job.
            if previous_end is not None:
                model.Add(start >= previous_end)
            previous_end = end

            task_interval = model.NewIntervalVar(start, duration, end, 'interval '+ suffix)
            jobs[j, stage] = task_interval

            if stage not in stage_tasks:
                stage_tasks[stage] = [task_interval]
            else:
                stage_tasks[stage].append(task_interval)

    
            # add constraint for duration for stage 2
            if stage == 2:

                l_lunch_batch = model.NewBoolVar('lunch '+ suffix)

                # starting time is evening  -- l_lunch_batch == False
                # evening batch take 6 hours
                model.Add(duration == paras['duration_2'][six_hour_idx]).OnlyEnforceIf(l_lunch_batch.Not())
                l_stage_2_evening_start = []
                for d in days:
                    # cannot start evening batch after the last day midnite
                    if d == paras['days'] - 1: continue
                    # if this task start at day d, make sure it start at 12pm
                    start_time_evening = paras['start_2'][six_hour_idx] + (d + day_index) * day_in_seconds
                    l_stage_2_evening_task_start = model.NewBoolVar('stage 2 evening in {} {}'.format(d, suffix))
                    model.Add(start == start_time_evening).OnlyEnforceIf(
                        [l_stage_2_evening_task_start, l_lunch_batch.Not()])
                    l_stage_2_evening_start.append(l_stage_2_evening_task_start)
                # if we have this job at evening, one of the evening start day must be chosen
                model.Add(sum(s for s in l_stage_2_evening_start) == 1).OnlyEnforceIf(l_lunch_batch.Not())

                if category[j] == 0:
                    # this job can start at both batch

                    # lunch batch take 2 hours
                    model.Add(duration == paras['duration_2'][two_hour_idx]).OnlyEnforceIf(l_lunch_batch)
                    # starting time is at noon
                    l_stage_2_lunch_start = []
                    for d in days:
                        # if this task start at day d, make sure it start at 12pm
                        start_time_lunch = paras['start_2'][two_hour_idx] + (d + day_index) * day_in_seconds
                        l_stage_2_lunch_task_start = model.NewBoolVar('stage 2 lunch in {} {}'.format(d, suffix))
                        model.Add(start == start_time_lunch ).OnlyEnforceIf([l_stage_2_lunch_task_start, l_lunch_batch])
                        l_stage_2_lunch_start.append(l_stage_2_lunch_task_start)
                    # if we have this job at lunch, one of the mid day must be chosen
                    model.Add(sum(s for s in l_stage_2_lunch_start) == 1).OnlyEnforceIf(l_lunch_batch)





                else: # category 1 can only happen at evening batch
                    model.Add(l_lunch_batch == 0)
            else:
                model.Add(duration == OpsDuration[j][stage])
            # for all tasks except stage 2, add constraint that end time before shift over at 6pm, start time after 8am for every task on the same day
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
        model.AddCumulative(stage_tasks[stage],[1] * len(stage_tasks[stage]), capacity[stage])

    # lets start with object to be makespan

    #print(f'starts {start_job}')
    #print(f'ends {end_job}')
    #print(f'durations { durations}')
    #print(f'stage tasks {stage_tasks}')

    # Makespan objective.
    #objective_choice = 'minimise_max_end_time'
    #objective_choice = 'minimise_max_case_duration'
    objective_choice = 'minimise_total_case_duration'
    if objective_choice == 'minimise_max_end_time':
        obj_var = model.NewIntVar(0, horizon, 'makespan')
        model.AddMaxEquality(obj_var, last_end)
        model.Minimize(obj_var)
    # try minimise total in system time
    if objective_choice == 'minimise_total_case_duration':
        # wieghted duration - floated job has more weights
        model.Minimize(sum((last_end[i] - paras['ready_time'][i]) * paras['job_weight'][i] for i in range(len(paras['job_data']))))
        #model.Minimize(sum(last_end[i]  for i in paras['allJobs']))
    # try minimise max in system time
    if objective_choice == 'minimise_max_case_duration':
        in_systems = []
        for j in range(len(paras['job_data'])):
            suffix = 'in_system_{j}'
            in_system_var = model.NewIntVar(0, horizon, suffix)
            model.Add(in_system_var ==  paras['job_weight'][j]*(last_end[j] - paras['ready_time'][j]))
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

        #need to output all start and end time for each job at each stage





        print(status)
        case_durations = []
        formatted_start_time = {} # indexed by job id and task id
        formatted_end_time = {}
        formatted_durations = {}
        x_pairs_dict = {} # indexed by stage id store (start_time, duration) for all jobs


        # get all jobs that not finished today
        currentDay = 0
        currentDayEndingTime = day_in_seconds * (day_index + 1)

        # I want to store unifinished jobs based on day index, only deal with jobs that not finished tmr for next day planning
        unfinished_jobs = {} # key = day, arrays of first unfinished stage
        task_type = collections.namedtuple('task', 'j_id j_index stage opsDuration category')
        for j in range(len(paras['job_data'])):
            floatted_flag = None
            for t in paras['allTasks']:
                start = start_job.get((j,t), None)

                if start is None: continue
                # if stage is 1 next day, this iis the floatted stge, if stage is 2 at noon next day, what now?
                # how to get floatted job? if this job is not finished today, what is finished?
                # start of any task of this job is after today
                if (t <=1 or t>=3) and solver.Value(start)>= starts_time[1] and floatted_flag is None:

                    # get day index store first unfinisheed stage based on day
                    floatted_day = solver.Value(start) // day_in_seconds
                    if floatted_day not in unfinished_jobs:
                        unfinished_jobs[floatted_day] = [task_type(j_id= j, j_index=paras['job_index'][j],
                                                                   stage=t, opsDuration=paras['job_data'][j], category=paras['category'][j])]
                    else:
                        unfinished_jobs[floatted_day].append(task_type(j_id= j, j_index=paras['job_index'][j],
                                                                       stage=t, opsDuration=paras['job_data'][j], category=paras['category'][j]))


                    floatted_flag = j
                    #print(f'adding job {j} at stage {t} with floattd_stage {floatted_stage}')



                #print(f'{j} {t}')
                print('start {}, duration {}, end {}'.format(solver.Value(start_job[j, t]), solver.Value(durations[j, t]), solver.Value(end_job[j, t])))
                start_time = format_time(solver.Value(start_job[j, t]))
                end_time = format_time(solver.Value(end_job[j, t]))

                formatted_start_time[j, t] = start_time
                formatted_end_time[j,t] = end_time
                duration = solver.Value(durations[j, t])
                formatted_durations[j,t] = duration
                
                if t not in x_pairs_dict:
                    x_pairs_dict[t] = [(solver.Value(start_job[j, t]), duration )]
                else:
                    x_pairs_dict[t].append((solver.Value(start_job[j, t]), duration ))
            # covert to decimal duration
            
            job_duration = (solver.Value(last_end[j]) - paras['ready_time'][j])/day_in_seconds

            # todo case duration is not correct in this veroion
            case_durations.append(job_duration)

        for key in unfinished_jobs:
            print(f'day {key} unfinished {unfinished_jobs[key]}')

        all_results = [] # each row - job id, stage , start, end duration
        for j in range(len(paras['job_data'])):

            for t in paras['allTasks']:
                start = formatted_start_time.get((j,t), None)
                if start is None: continue

                # store job task info finished before 6pm for except stage 2, and mid nite

                job_data = ['job_{i}'.format(i=paras['job_index'][j])]
                end = formatted_end_time.get((j,t), None)

                if ((t <=1 or t>=3) and solver.Value(end_job[j, t]) <= ends_time[0]) \
                        or (t==2 and solver.Value(end_job[j,t]) <= currentDayEndingTime + 8 * seconds_per_hour):
                    duration = formatted_durations.get((j,t), None)
                    job_data = job_data + [t, start, end, duration]
                    all_results.append(job_data)
        # this is useful to calculate performace
        df = pd.DataFrame(all_results, columns=['job', 'stage', 'start', 'end', 'duration'])
        df[["start", "end"]] = df[["start", "end"]].apply(pd.to_datetime)
        print(df.head)
        print(df.info())

        paras['result'].append(df)
        
        
        # I want to know all jobs start at least next day
        time_filter_tmr = format_time((day_index + 1) * day_in_seconds + 8 * seconds_per_hour)
        time_ending_tmr = format_time((day_index + 1) * day_in_seconds + 18* seconds_per_hour)
        tmr_floatted = df[(df['start'] >= time_filter_tmr) & (df['end'] < time_ending_tmr)]

        #fisrt_staged_float = tmr_floatted[ tmr_floatted['start'] = )
        print(tmr_floatted.info())
        print('max start', df['start'].max())

        # lost track of what to do...want to store all jobs?
        # what do to ? what to do?

        

        # now what,...these jobs

        #for j in range(len(paras['job_data'])):
        #    print(f'{case_durations[j]}')

        #print('formmated starting time')
        #for key in formatted_start_time:
        #        print(formatted_start_time[key])

        #print('formmated ending time')
        #for key in formatted_end_time:
        #        print(formatted_end_time[key])
                
        # todo average case duration is not correct in this floatted version
        if objective_choice == 'minimise_total_case_duration':
            print('average duration {}'.format(solver.ObjectiveValue() / day_in_seconds / paras['max_jobs']))
        else: print('max in_system duration =', solver.ObjectiveValue() / day_in_seconds)
        if status == cp_model.FEASIBLE: print('Fesible')
        else: print('Optimal')

        for stage in paras['allTasks']:
            start = x_pairs_dict.get(stage, None)

            if start is None: continue
            x_pairs = x_pairs_dict[stage]
            day_start_times = [ (d + day_index) * day_in_seconds for d in days] + [ (d+1 + day_index) * day_in_seconds]
            #draw.draw_gannt(x_pairs, stage, day_start_times, day_index)

            intervals = []
            for x in x_pairs:
                intervals.append(pd.Interval(x[0], x[0] + x[1]))
            #print('intevals are', intervals)
            ovrelapped_flag = False
            for idx, a in enumerate(intervals):
                for idx1, b in enumerate(intervals):
                    if idx1 <= idx: continue
                    if (a.overlaps(b)):
                        ovrelapped_flag = True
            #print('is there overlap ', ovrelapped_flag)
        # now crated floatted jobs
        floatted_job_ops_duration = []
        floatted_job_ready_time = []
        floatted_job_start_stage = []
        floatted_job_category = []

        key = day_index + 1

        floatted_jobs = unfinished_jobs.get(key, [])

        for job in floatted_jobs:
            floatted_job_ops_duration.append(job.opsDuration)
            floatted_job_ready_time.append(currentDayEndingTime + 8 * seconds_per_hour) # ready next day 8am
            floatted_job_start_stage.append(job.stage)
            floatted_job_category.append(job.category)

            
        paras['job_data'] = floatted_job_ops_duration
        paras['ready_time'] = floatted_job_ready_time
        paras['stage'] = floatted_job_start_stage
        paras['category'] = floatted_job_category
        paras['job_weight'] = [2] * len(floatted_job_category)
        paras['number_floatted'] = len(floatted_job_category)

        print('new job data {}'.format(paras['job_data'] ))
        print('new ready_time {}'.format(paras['ready_time']))
        print('new stage {}'.format(paras['stage']))
        print('new category {}'.format(paras['category']))
        print('job_weight {}'.format(paras['job_weight']))
        print('floatted num of jobs', len(floatted_job_category))
        

load_real_date()


for day in range(0,0):
    read_data(day)
    lab_model(day)

#df = pd.concat(paras['result'])
#df.to_csv('out.csv', index=False)

