# in Cplex constraint programming, there is a state function which can specify the state over intervals. We implement alwaysEqual version in python or-tools here
# writer: oddo zhang
from ortools.sat.python import cp_model

jobs_data = [  # ( processing_time，state).
    (10, 1),
    (4, 2),
    (14, 2)
]
release_time = [0, 11, 16]

# lexico ordering, start time, end time
def lexicoOrdering(elem):
    return (elem[0], elem[1])


def getValues(var, solver):
    x = []
    x.append(solver.Value(var[0]))
    x.append(solver.Value(var[1]))
    return x

class StateFunction():
    def __init__(self):
        self.stateDict = {}


    def alwaysEqual(self, start_var, end_var, interval, stateVal, model):
        # implement a dict for each value
        newStateValue = False
        if stateVal not in self.stateDict:
            self.stateDict[stateVal] = [[start_var, end_var, interval]]
            newStateValue = True
            # for this given value, make sure the interval does not collide with other intervals for other value
            # basically it is no overlap?
        for key, vars in self.stateDict.items():
            if key != stateVal:
                for var in vars:
                    # add no overlap interval with different values, cos they cannot overlap otherwise state constraint violtates
                    model.AddNoOverlap([interval, var[2]])
            else:  # for same key, just append this pair
                if newStateValue == False:
                    self.stateDict[key].append([start_var, end_var, interval])
    # print(statedict)

    def printState(self, solver):
        for key, vars in self.stateDict.items():
            varValues = list(map(lambda p: getValues(p, solver), vars))
            varValues.sort(key=lexicoOrdering)

            valueIntervals = []
            startValue = varValues[0][0]
            endValue = varValues[0][1]

            valueIntervals.append([startValue, endValue])
            idx = 1
            while idx < len(varValues):
                # continues
                if varValues[idx][0] == startValue or varValues[idx][0] < endValue:
                    endValue = varValues[idx][1]
                    valueIntervals[-1] = [startValue, endValue]
                else:  # a discrete start value
                    startValue = varValues[idx][0]
                    endValue = varValues[idx][1]
                    valueIntervals.append([startValue, endValue])
                idx += 1
            print('state = ', key, valueIntervals)





        # start = solver.Value(var[0])
        # end = solver.Value(var[1])
        # print('state = ', key, 'start = ', start,' end= ', end)


def stateCPModel():
    state = StateFunction()
    model = cp_model.CpModel()
    jobs = len(jobs_data)
    alljobs = range(jobs)
    jobintervals = []
    overlaps = {}
    overlapIntervals = {}
    starts = []
    ends = []
    for i in alljobs:
        overlaps[i] = []
        for j in alljobs:
            if i > j: continue
            if jobs_data[i][1] != jobs_data[j][1]:
                overlaps[i].append(j)
    # if state of two intervals are different, they cannot overlap
    for j in alljobs:
        duration = jobs_data[j][0]
        suffix = '_%i' % (j)
        start_var = model.NewIntVar(release_time[j], 77, 'start' + suffix)
        end_var = model.NewIntVar(release_time[j] + duration, 77, 'end' + suffix)
        interval_var = model.NewIntervalVar(start_var, duration, end_var, 'interval' + suffix)
        jobintervals.append(interval_var)
        ends.append(end_var)
        starts.append(start_var)

        state.alwaysEqual(start_var, end_var, interval_var, jobs_data[j][1], model)
    # for i in alljobs:
    #    overlapIntervals[i] = []
    #    for j in alljobs:
    #        if i > j: continue
    #        if j in overlaps[i]:
    #            overlapIntervals[i].append(jobintervals[j])
    #    overlapIntervals[i].append(jobintervals[i])
    # print(overlapIntervals)
    # no overlap
    # for i in alljobs:
    #    print(overlaps[i])
    #    if overlaps[i] == None: continue
    #    for j in overlaps[i]:
    #        print(i, j)
    #        suffix = '_%i_%i' % (i, j)
    #        iFinishedBeforej = model.NewBoolVar(suffix)

    #        model.Add(starts[j] > ends[i] + 20).OnlyEnforceIf(iFinishedBeforej)
    #        model.Add(starts[i] > ends[j] + 2).OnlyEnforceIf(iFinishedBeforej.Not())

    # model.AddNoOverlap(overlapIntervals[i])
    # state function respection

    # Makespan objective.
    obj_var = model.NewIntVar(0, 80, 'makespan')
    model.AddMaxEquality(obj_var, [
        ends[job_id]
        for job_id in alljobs
    ])
    model.Minimize(obj_var)
    # Solve model.
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    if status == cp_model.OPTIMAL:
        # Create one list of assigned tasks per machine.
        for job_id, job in enumerate(jobs_data):
            print('job id', job_id, solver.Value(starts[job_id]), solver.Value(ends[job_id]))

        state.printState(solver)


stateCPModel()
