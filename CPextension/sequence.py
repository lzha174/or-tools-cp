import collections

from ortools.sat.python import cp_model
# in this file, we implment sequence function
# so we can constrint start, finish and extra features for a sequence
# use a hamliton path to find ranks to determine the sequence
def create_model():
    model = cp_model.CpModel()
    nbJobs = 20
    nbMchs = 5
    OpDurations = [
        [62, 22, 77, 88, 39],
        [68, 94, 66, 57, 48],
        [57, 6, 20, 24, 86],
        [54, 37, 87, 50, 78],
        [50, 80, 62, 60, 58],
        [59, 78, 79, 93, 88],
        [36, 55, 10, 13, 43],
        [46, 81, 36, 13, 36],
        [83, 67, 39, 1, 88],
        [23, 54, 25, 8, 2],
        [2, 57, 82, 63, 16],
        [38, 20, 93, 15, 13],
        [82, 51, 66, 89, 63],
        [9, 34, 42, 42, 46],
        [76, 25, 13, 13, 23],
        [99, 34, 77, 24, 41],
        [76, 23, 96, 56, 84],
        [12, 94, 2, 5, 9],
        [13, 84, 57, 78, 72],
        [19, 86, 6, 58, 27]
    ]
    Jobs = range(nbJobs)
    Mchs = range(nbMchs)
    intervals_per_machines = collections.defaultdict(list)
    intervals_per_jobs = collections.defaultdict(list)
    ends_per_jobs =  []
    for j in Jobs:
        for m in Mchs:
            suffix = r'job_%i_mch_%i' % (j, m)

            #print(suffix)
            start_var = model.NewIntVar(0, 9999, 'start '+ suffix)
            duration = OpDurations[j][m]
            end_var = model.NewIntVar(0, 9999, 'end' + suffix)
            job_interval = model.NewIntervalVar(start_var, duration, end_var, suffix)
            intervals_per_machines[m].append(job_interval)
            intervals_per_jobs[j].append((start_var, end_var))
            if m == 4:
                ends_per_jobs.append(end_var)
    # each job no overlap on each machine
    print(ends_per_jobs)
    print(intervals_per_machines)
    for m in Mchs:
        model.AddNoOverlap(intervals_per_machines[m])
    for j in Jobs:
        for i in range(len(intervals_per_jobs[j]) - 1) :
            #print(intervals_per_jobs[j][i + 1][0], intervals_per_jobs[j][i][1])
            model.Add(intervals_per_jobs[j][i + 1][0] > intervals_per_jobs[j][i][1])

    # Objective.
    objective_var = model.NewIntVar(0, 9999, 'obj')
    model.AddMaxEquality(objective_var, ends_per_jobs)
    model.Minimize(objective_var)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 20
    print('solve', cp_model.FEASIBLE)
    status = solver.Solve(model)
    print(status,  cp_model.FEASIBLE)
    if status == cp_model.OPTIMAL or  status == cp_model.FEASIBLE:
        print(solver.Value(objective_var))
create_model()