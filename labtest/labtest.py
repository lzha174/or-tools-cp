import collections

from ortools.sat.python import cp_model
from google.protobuf import text_format

import drawing.draw
from drawing import draw

import pandas as pd

paras = {
    'job_data':[],
    'process_category': [0,	1,	1,	0,	0,	1,	1,	1,	1,	1],# 0 means it can be at 12-2pm or 6pm-3am batch, 1 means it only be at 6pm-3pm batch, processing time is 2 hour or 9 hours
    'days': 3, # planning for 3 days
    'start':8, # start time for stage 0, 1, 3 every day, 8am
    'end': 18, # end time for stage 0,1,3 every day 6pm
    'start_2': [12 * 60, 18 * 60], # start time for category 0 and 1 at stage 2, 12pm, and 6 pm
    'duration_2': [2 *60 , 9 * 60] # duration for category 0 and 1 at stage 2 in minutes
}

def process_data(row):
    data = [row['stage_0'], row['stage_1'], row['stage_3']] # processing time in minutes at every stage escept stage 2, fixed time
    paras['job_data'].append(data)

def read_data():

    df = pd.read_csv ('job_data.csv')
    #df = df.drop(columns = ['stage_2'])
    print(df.head)
    df.apply(process_data, axis = 1)
    print(paras['job_data'])

    nbJobs = df.shape[0]
    allJobs = range(nbJobs)
    print('nb jobs {}'.format(nbJobs))

    numTasks = 4
    allTasks = range(numTasks)

    paras['nbJobs'] = nbJobs
    paras['allJobs'] = allJobs
    paras['numTasks'] = numTasks
    paras['allTasks'] = allTasks

def lab_model():
    # every job has 4 tasks, third task is processed in batch, capacity is 1000
    # every task of job start time and end time for stage 0, 1, 3 must fall in the normal working hours 8am - 6 pm  every day
    # every task for stage 2 must start at 12pm or 6pm, first start lasts 2 hours, second start lasts 9 hours
    # a job has category 0 can start stage 2 at both time slots, with category 1 can only start at evening slot 6pm

    # define starting time, ending time in mins for 3 days

    OpsDuration = paras['job_data']
    maxDuration = 540

    category = paras['process_category']
    two_hour_idx = 0
    six_hour_idx = 1

    days = range(paras['days'])
    starts_time = []
    ends_time = []
    for d in days:
        start = 8  * 60 + d * 24 * 60
        end = 18 * 60 + d * 24 * 60
        starts_time.append(start)
        ends_time.append(end)
    print(f'starting times are {starts_time}')
    print(f'ending times are {ends_time}')

    model = cp_model.CpModel()
    horizon = ends_time[-1]
    print(f'horizon {horizon}')

    # define task intervals for each job

    # starting time of tasks of each job
    start_job = {} # index by job_id, array of tasks
    # ending time of tasks of each job
    end_job = {}
    durations = {}
    jobs = {} # intervals index by job_id, task id
    stage_tasks = {} # indexed by stage id
    for j in paras['allJobs']:
        start_job[j] = []
        end_job[j] = []
        durations[j] = []

        previous_end = None

        for stage in paras['allTasks']:
            suffix = f'job {j} task {stage}'
            start = model.NewIntVar(0, horizon, 'start ' + suffix)

            duration = model.NewIntVar(0, maxDuration, 'duration' + suffix)

            end = model.NewIntVar(0, horizon, 'end ' + suffix)

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
                if category[j] == 0:
                    # this job can start at both batch
                    l_lunch_batch = model.NewBoolVar('lunch '+ suffix)
                    # lunch batch take 2 hours
                    model.Add(duration == paras['process_category'][two_hour_idx]).OnlyEnforceIf(l_lunch_batch)
                    # starting time is at noon
                    l_stage_2_lunch_start = []
                    for d in days:
                        # if this task start at day d, make sure it start at 12pm
                        start_time_lunch = paras['start_2'][two_hour_idx] + d * 24 * 1440
                        l_stage_2_lunch_task_start = model.NewBoolVar('stage 2 lunch in {} {}'.format(d, suffix))
                        model.Add(start == start_time_lunch ).OnlyEnforceIf([l_stage_2_lunch_task_start, l_lunch_batch])
                        l_stage_2_lunch_start.append(l_stage_2_lunch_task_start)
                    # if we have this job at lunch, one of the mid day must be chosen
                    model.Add(sum(s for s in l_stage_2_lunch_start) == 1).OnlyEnforceIf(l_lunch_batch)

                    # starting time is evening
                    # evening batch take 6 hours
                    model.Add(duration == paras['process_category'][six_hour_idx]).OnlyEnforceIf(l_lunch_batch.Not())
                    l_stage_2_evening_start = []
                    for d in days:
                        # if this task start at day d, make sure it start at 12pm
                        start_time_evening = paras['start_2'][six_hour_idx] + d * 24 * 1440
                        l_stage_2_evening_task_start = model.NewBoolVar('stage 2 evening in {} {}'.format(d, suffix))
                        model.Add(start == start_time_evening ).OnlyEnforceIf([l_stage_2_evening_task_start, l_lunch_batch.Not()])
                        l_stage_2_evening_start.append(start_time_evening)
                    # if we have this job at evening, one of the evening start day must be chosen
                    model.Add(sum(s for s in l_stage_2_evening_start) == 1).OnlyEnforceIf(l_lunch_batch.Not())



                else: # category 1 can only happen at evening batch
                    model.Add(duration == paras['process_category'][six_hour_idx])
                    l_stage_2_evening_start = []
                    for d in days:
                        # if this task start at day d, make sure it start at 12pm
                        start_time_evening = paras['start_2'][six_hour_idx] + d * 24 * 1440
                        l_stage_2_evening_task_start = model.NewBoolVar('stage 2 evening in {} {}'.format(d, suffix))
                        model.Add(start == start_time_evening ).OnlyEnforceIf(l_stage_2_evening_task_start)
                        l_stage_2_evening_start.append(start_time_evening)
                    # if we have this job at evening, one of the evening day must be chosen
                    model.Add(sum(s for s in l_stage_2_evening_start) == 1)
                # what about the starting time for this task
            else:
            # for all tasks except stage 2, add constraint that end time before shift over at 6pm, start time after 8am for every task on the same day
                l_in_day = {}
                for d in days:
                    l_in_d = model.NewBoolVar('task of {j} in {d}')
                    model.Add(start >= starts_time[d]).OnlyEnforceIf(l_in_d)
                    model.Add(end <= ends_time[d]).OnlyEnforceIf(l_in_d)
                    l_in_day[d] = l_in_d
                # one of the days must be chosen
                model.AddBoolXOr([item for key, item in l_in_day.items()])



            start_job[j].append(start)
            end_job[j].append(end)
            durations[j].append(duration)
    # add capacity constraint for stage 0
    # here the machine capacity 1 means for each machine, it can only perform one job at the same time
    # for stage 2, we have 1000 machines
    capacity = [1, 2, 1000, 2]
    for stage in paras['allTasks']:
        # capacity is 1, means no tasks at the same stage can overlap
        # capacity is 2, means two tasks can overlap, as we have two machines
        # stage 2 has 1000 machines, ha
        model.AddCumulative(stage_tasks[stage],[1] * len(stage_tasks[stage]), capacity[stage])


    print(f'starts {start_job}')
    print(f'ends {end_job}')
    print(f'durations { durations}')
    print(f'stage tasks {stage_tasks}')



# step 1 see if cumulative works for optional job
def create_model():
    jobs = [10, 20, 10] # duration
    machines = 2
    allJobs = range(len(jobs))
    allMachines = range(machines)

    model = cp_model.CpModel()
    horizon = 80
    # a job can be done on any of two machine
    task_type = collections.namedtuple('task', 'start end duration present interval')
    jobVars = []
    starts = []
    ends = []
    
    job_starts = {}  # indexed by (job_id).
    job_presences = {} # indexed by (job_id, machine_id)

    # store jobs on each machine
    intervals_per_machines = collections.defaultdict(list)
    presences_per_machines = collections.defaultdict(list)
    # store start on each machine
    starts_machines = {} # (job_id, machine_id)
    ends_machines = {} # job_id, machine_id
    # store end on each machine
    ends_per_machines = collections.defaultdict(list)


    # create job intervals
    for idx in allJobs:
        start = model.NewIntVar(0, horizon, name='start of job %i' % (idx))
        duration = jobs[idx]
        end = model.NewIntVar(0, horizon, name = 'end of job %i' % (idx))
        j_interval = model.NewIntervalVar(start, duration, end, name = 'job %i' % (idx))
        #jobVars[idx] = task_type(start= start_v, end = end_v, duration = duration, present = True, interval = j_interval)
        jobVars.append(j_interval)
        starts.append(start)
        ends.append(end)
        #remember start of each job
        job_starts[idx] = start
    
        # create optional jobs:
        l_presences = []
        for mid in allMachines:
                suffix = 'optional of {} on m {}'.format(idx, mid)
                l_start = model.NewIntVar(0, horizon, name='start '+suffix)
                l_presence = model.NewBoolVar('bool' + suffix)
                l_presences.append(l_presence)
                l_duration = jobs[idx]
                l_end = model.NewIntVar(0, horizon, name = 'end ' + suffix)
                optional_interval = model.NewOptionalIntervalVar(l_start, l_duration, l_end, l_presence, suffix)

                # Link the master variables with the local ones.
                model.Add(start == l_start).OnlyEnforceIf(l_presence)
                model.Add(duration == l_duration).OnlyEnforceIf(l_presence)
                model.Add(end == l_end).OnlyEnforceIf(l_presence)

                #add interval to the correct machine
                intervals_per_machines[mid].append(optional_interval)
                starts_machines[idx, mid] = l_start
                ends_machines[idx, mid] = l_end
                
                job_presences[idx, mid] = l_presence

            # one of machine must perform this job
        model.Add(sum(l_presences) == 1)
    
    for mid in allMachines:
        model.AddCumulative(intervals_per_machines[mid], [1] * len(intervals_per_machines[mid]), 1)
    #    model.AddNoOverlap(intervals_per_machines[mid])
    # add that machine capacity is 2
    #model.AddCumulative(jobVars, [1] * len(jobVars), 2)

    print(starts_machines)

    # minimise total processing time
    makespan = model.NewIntVar(0, 80, 'makespan')
    model.AddMaxEquality(makespan, ends)

    #model.Minimize(sum(ends[i] - starts[i] for i in allJobs))
    model.Minimize(makespan)

    # Solve.
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60 * 60 * 2
    status = solver.Solve(model)
    # Print solution.
    if status == cp_model.FEASIBLE or status == cp_model.OPTIMAL:
        x_pairs = []
        for job_id in allJobs:
            start_value = solver.Value(starts[job_id])
            duration = jobs[job_id]
            x_pairs.append((start_value, duration))
            end_value = solver.Value(ends[job_id])
            print(start_value, end_value)
            for mid in allMachines:
                if solver.BooleanValue(job_presences[job_id, mid]):
                    print('perform job {}  on machine {}'.format(job_id, mid))
                    print(solver.Value(starts_machines[job_id, mid]), solver.Value(ends_machines[job_id, mid]))

        draw.draw_gannt(x_pairs)

read_data()
lab_model()