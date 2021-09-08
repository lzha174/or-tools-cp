from ortools.sat.python import cp_model
import collections
# In this script, we implement AddNoOverlap with minimum transition time between two intervals. This is missing in OR-TOOLS CP-SAT package
# writer:oddo zhang
jobs_data = [  # ( processing_time).
    10,
    4,
    1
]

transitionMatrix = [[0, 1, 3], [2, 0, 5], [1, 5, 0]]
release_time = [0, 10, 16]


class ModelTransition(cp_model.CpModel):
    def AddNoOVerlapTransition(self, intervalList):
        for idx in range(len(intervalList)):
            for idx1 in range(idx + 1, len(intervalList)):
                suffix = '_%i_%i' % (intervalList[idx].index, intervalList[idx1].index)
                # if first interval finish before second interval, end time of first must be smaller than the second
                # use a bool to make this implication constraint
                finishedBefore = self.NewBoolVar(suffix)
                self.Add(intervalList[idx1].start > intervalList[idx].end + transitionMatrix[idx][idx1]).OnlyEnforceIf(
                    finishedBefore)
                # the other way around
                self.Add(intervalList[idx].start > intervalList[idx1].end + transitionMatrix[idx1][idx]).OnlyEnforceIf(
                    finishedBefore.Not())


interval_data = collections.namedtuple('interval_data', 'start end interval index')


def createModel():
    jobs = len(jobs_data)
    alljobs = range(jobs)
    jobintervals = []

    model = ModelTransition()
    maxTransition = max(max(transitionMatrix))
    maxRelease = max(release_time)
    maxDuration = max(jobs_data)

    horizon = (maxRelease + maxDuration + maxTransition + 1) * len(jobs_data)
    for j in alljobs:
        duration = jobs_data[j]
        suffix = '_%i' % (j)
        start_var = model.NewIntVar(release_time[j], horizon, 'start' + suffix)
        end_var = model.NewIntVar(release_time[j] + duration, horizon, 'end' + suffix)
        interval_var = model.NewIntervalVar(start_var, duration, end_var, 'interval' + suffix)
        # jobintervals.append((interval_var, j))
        jobintervals.append(interval_data(start=start_var,
                                          end=end_var,
                                          interval=interval_var,
                                          index=j))

    model.AddNoOVerlapTransition(jobintervals)

    # Makespan objective.
    obj_var = model.NewIntVar(0, horizon, 'makespan')
    model.AddMaxEquality(obj_var, [
        jobintervals[job_id].end
        for job_id in alljobs
    ])
    model.Minimize(obj_var)
    # Solve model.
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    if status == cp_model.OPTIMAL:
        for job_id, job in enumerate(jobs_data):
            print('job id', job_id, solver.Value(jobintervals[job_id].start), solver.Value(jobintervals[job_id].end))


createModel()
