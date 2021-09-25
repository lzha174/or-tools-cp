# in this model, we allow batch scheduling: multiple jobs can be processed on the machine, but they must start at the same time
#  a job in a different batch must not overlap with any job in other batch
# a machine can do 2 jobs at a time

from ortools.sat.python import cp_model
import collections
import pandas as pd
import matplotlib.pyplot as plt

jobs_data = [  # ( processing_time on a machine).
    5,
    3,
    4,
    4,
    12
]

# cannto start machine 2 if there is only one item finished..at least two items finsihed
interval_data = collections.namedtuple('interval_data', 'start end')

def create_model():
    model = cp_model.CpModel()
    jobs = len(jobs_data)
    alljobs = range(jobs)
    jobsVars = {}
    numMachinews = 1
    allMachines = range(numMachinews)


    machineJobs = []
    for m in allMachines:
        for j in alljobs:
            suffix = '_%i_%i' % (j,m)
            start_var = model.NewIntVar(0, 77, 'start' + suffix)
            end_var = model.NewIntVar(0, 77,'end' + suffix)
            jobVar = model.NewIntervalVar(start_var,jobs_data[j],end_var,name = 'job' + suffix)
            jobsVars[j] = interval_data(start=start_var, end=end_var)
            machineJobs.append(jobVar)

    for i in alljobs:
        for j in alljobs:
            if i == j:continue
            inBatch = model.NewBoolVar(name='%i_%i_in' % (i,j))
            # if in same batch
            model.Add(jobsVars[i].start == jobsVars[j].start).OnlyEnforceIf(inBatch)
            iBeforeJ = model.NewBoolVar('%i_%i_b4' % (i,j))
            jBeforeI = model.NewBoolVar('%i_%i_b4' % (j, i))
            # USE TWO BOOLS, SO BOTH OF THEM CAN BE FALSE, IF I USE ONE BOOL, BOOL.NOT, BOOL ONE OF THEM MUST BE TRUE
            model.Add(jobsVars[i].end <= jobsVars[j].start).OnlyEnforceIf(iBeforeJ)
            model.Add(jobsVars[j].end <= jobsVars[i].start).OnlyEnforceIf(jBeforeI)
            # if two jobs in the same batch, these two bools must be false so no constraints are defined above
            model.AddImplication(inBatch, iBeforeJ.Not())
            model.AddImplication(inBatch, jBeforeI.Not())
            # if not in the same batch, one of these two bools must be true
            model.AddBoolOr([iBeforeJ,jBeforeI]).OnlyEnforceIf(inBatch.Not())
    # a machine can do 2 jobs same time
    model.AddCumulative(machineJobs,[1 for j in alljobs],2)

    obj_var = model.NewIntVar(0, 77, 'makespan')
    model.AddMaxEquality(obj_var, [
        jobsVars[j].end
        for j in alljobs
    ])
    model.Minimize(obj_var)
    # Solve model.
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    print(status)
    df = pd.DataFrame()
    if status == cp_model.OPTIMAL:
        starts = [solver.Value(jobsVars[j].start) for j in alljobs]
        ends = [solver.Value(jobsVars[j].end) for j in alljobs]
        datas = []
        for j in alljobs:
            data = [str(j),str(starts[j]),str(ends[j])]
            datas.append(data)
        df = pd.DataFrame(datas, columns=['Task','Start','Finish'])
        for job_id in alljobs:
            print('job id', job_id, solver.Value(jobsVars[job_id].start), solver.Value(jobsVars[job_id].end))

        print('min span', solver.ObjectiveValue())
        print(df.head)


    fig, ax = plt.subplots()
    startY = 10
    colors = ['blue','red','yellow','green','orange']
    for j in alljobs:

        ax.broken_barh([(starts[j], jobs_data[j])], (startY, 9), facecolors=colors[j])
        startY += 10

    ax.set_ylim(5, 65)
    ax.set_xlim(0, 20)
    ax.set_xlabel('seconds since start')
    ax.set_yticks([15 + 10*j for j in alljobs])
    ax.set_yticklabels(['job '+str(j) for j in alljobs])
    ax.grid(True)

    plt.show()



create_model()