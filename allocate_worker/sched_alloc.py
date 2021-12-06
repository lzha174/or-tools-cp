# this file models workers belonging to seperate work groups
# each group can perform some of the tasks
# each group, workers has a max unused number
# a task must be done by one of the allowed group
# we need to decid worker interval so that they can cover all jobs for the associated group
# this is acheived by cumulative functions difference bewteen avaliable resoruces and used resources

import collections

# Import Python wrapper for or-tools CP-SAT solver.
from ortools.sat.python import cp_model


class ModelComulative(cp_model.CpModel):
    def __init__(self):
        cp_model.CpModel.__init__(self)
        self.comulatives = {}
        self.max_capacity = 10

    def AddComulativeCustom(self, intervals, demands, horizon, name, consdier_present = False ):
        comulative_vars = [self.NewIntVar(0, self.max_capacity, 'cum '+ name +' '+ str(t)) for t in range(0, horizon )]
        # bacilly cumulative constraint is very simple
        # create a intvar for every discrete timestamp in the horizon, at every stamp, if an interval contains this timestamp
        # add the demand for that interval, the sum will be equal to the intvar at that time stamp
        # when we constrain capacity, go through each timestap and add a constraint for capacity
        # if we have multiple cumulative vars, we can even do add or subtract between these vars for each time stamp
        #print(comulative_vars)
        for t in range(0, horizon):
            totalDemand = 0
            in_range_vars = [self.NewIntVar(0, 1, 'in '+ name +' '+ str(i) + str(t)) for i in range(len(intervals))]
            for idx, interval in enumerate(intervals):
                # only consider this interval if it is present
                # if present = false, consdier = false
                # if consdier the false, skip this no constraint at all
                present = True
                if consdier_present:
                    present = interval.present
                # if interval.start <= t < interval.end total = +demand[interval]
                in_range = self.NewBoolVar(name + str(idx) + 'in' + str(t))
                # I like the OnlyEnfoceIf can use a list of literals, all of them must be true.
                # I can pass True/Fasle into the list
                self.Add(in_range_vars[idx] == 1).OnlyEnforceIf([in_range, present])
                self.Add(t - interval.start >= 0).OnlyEnforceIf([in_range, present])
                self.Add(interval.end - t > 0).OnlyEnforceIf([in_range, present])

                before_t = self.NewBoolVar(str(idx) + 'b4' + name +' '+ str(t))
                self.Add(interval.end <= t).OnlyEnforceIf([before_t,present])
                self.Add(in_range_vars[idx] == 0).OnlyEnforceIf([before_t, present])

                after_t = self.NewBoolVar(str(idx) + 'after' + name + ' ' + str(t))
                self.Add(interval.start > t).OnlyEnforceIf([after_t, present])
                self.Add(in_range_vars[idx] == 0).OnlyEnforceIf([after_t, present])

                self.AddBoolOr([in_range, before_t, after_t])

            self.Add(cp_model.LinearExpr.ScalProd(in_range_vars, demands) == comulative_vars[t])

        self.comulatives[name] = comulative_vars
    def difference(self, name1, name2, min_v, max_v):
        horizon = len(self.comulatives[name2])
        for t in range(horizon):
            print(self.comulatives[name1][t],self.comulatives[name2][t]  )
            self.Add(self.comulatives[name1][t] - self.comulatives[name2][t] >= min_v)
            self.Add(self.comulatives[name1][t] - self.comulatives[name2][t]  <= max_v)
    def show_cum(self, solver):
        for key in self.comulatives:
            print('key=',key)
            for item in self.comulatives[key]:
                print('cum value', solver.Value(item))
def create_model():
    tasks = ["masonry",
             "carpentry",
             "plumbing",
             "ceiling",
             "roofing",
             "painting",
             "windows",
             "facade",
             "garden",
             "moving" ]
    durations = [7, 3, 8, 3, 1, 2, 1, 2, 1, 1]
    starts = [0, 7, 7, 7, 10, 10, 11, 15, 15, 17]
    groups = ['g1','g2','g3']
    maxUnusedWorkers = [2, 1, 1]
    minUsedWorkers = [1,0,0]
    maxUsedWorkers = [3,1,1]
    mayperform = {"masonry":['g1','g2'], "carpentry":['g1','g3'], "plumbing":['g2'],
             "ceiling":['g1','g3'],
             "roofing":['g1','g3'],
             "painting":['g2','g3'],
             "windows":['g1','g3'],
             "facade":['g1','g2'],
             "garden":['g1','g2','g3'],
             "moving":['g1','g3']}
    workers = ['Thomas', 'Brett', 'Matthew', 'Scott', 'Bill']
    workersGroup =  {
    "g1": ["Thomas", "Brett", "Matthew"],
    "g2": ["Scott"],
    "g3": ["Bill"]}

    horizon = sum(duration for duration in durations)

    model = ModelComulative()
    solver = cp_model.CpSolver()
    #status = solver.Solve(model)

    # Named tuple to store information about created variables.
    task_type = collections.namedtuple('task', 'start end duration present interval')
    all_tasks = {}
    # create task interval
    for idx in range(len(tasks)):
        duration = durations[idx]
        start = starts[idx]
        end = start + duration
        suffix = '_%i' % (idx)

        interval_var = model.NewIntervalVar(start, duration, end,
                                            'interval' + suffix)
        all_tasks[idx] = task_type(start=start,
                                               end=end,duration=duration, present=0,
                                               interval=interval_var)
    all_optional_taks = {}
    for key, items in mayperform.items():
        idx = tasks.index(key)
        #print(idx)
        duration = durations[idx]
        start = starts[idx]
        end = start + duration
        all_optional_taks[idx] = {}
        for item in items:
            group_idx = groups.index(item)
            suffix = '_option_%i_%i' % (idx, group_idx)
            is_present = model.NewBoolVar('interval_bool' + suffix)
            optional_interval_var = model.NewOptionalIntervalVar(start, duration, end, is_present, 'interval' + suffix)
            all_optional_taks[idx][group_idx] = (task_type(start=start,
                                               end=end, present=is_present, duration = duration,
                                               interval=optional_interval_var))
    all_workers = {}

    for idx in range(len(workers)):
        suffix = '_%i' % (idx)
        start_var = model.NewIntVar(0, horizon, 'start' + suffix)
        end_var = model.NewIntVar(0, horizon, 'end' + suffix)
        duration_var = model.NewIntVar(0,horizon,'duration'+suffix)
        inteval_var = model.NewIntervalVar(start_var,duration_var,end_var,'worker'+suffix)
        all_workers[idx] = task_type(start = start_var, end = end_var, duration = duration_var, present=0, interval = inteval_var)

    # constraint alternative:
    for key in all_tasks:
        model.Add(cp_model.LinearExpr.Sum([all_optional_taks[key][group_idx].present for group_idx in all_optional_taks[key]]) == 1)
    # cumlative this is the hard part i dont know how to model negative demand
    for group in workersGroup:
        group_idx = groups.index(group)
        # for this group
        print('group index', group_idx)
        # for each group,
        workers_this_group = []

        group_worker_name = 'group_' + str(group_idx) + '_worker'
        
        for worker in workersGroup[group]:
            worker_idx = workers.index(worker)
            workers_this_group.append(all_workers[worker_idx])
        print('worker in this group',workers_this_group)
        model.AddComulativeCustom(workers_this_group, [1]*len(workers_this_group), horizon, group_worker_name)

        optional_interval_this_group = []
        # how to get all optional tasks for this group
        # i alrady have group index in this loop
        # need to loop through all task index
        group_job_name = 'group_'+str(group_idx)+'_option'
        for task_idx in range(len(tasks)):
            if (task_idx in all_optional_taks) and (group_idx in all_optional_taks[task_idx] ) :
                print('in this group', all_optional_taks[task_idx][group_idx].interval)
                optional_interval_this_group.append(all_optional_taks[task_idx][group_idx])
        model.AddComulativeCustom(optional_interval_this_group, [1] * len(optional_interval_this_group), horizon, group_job_name, True)
        print('add ', group_worker_name, group_job_name, maxUnusedWorkers[group_idx])
        model.difference(group_worker_name, group_job_name, 0, maxUnusedWorkers[group_idx])
    #def AddCumulative(self, intervals, demands, capacity)
    # MInimise max length of worker
    obj_var = model.NewIntVar(0, horizon, 'makespan')
    model.AddMaxEquality(obj_var, [
        all_workers[idx].duration for idx in all_workers
    ])
    model.Minimize(obj_var)

    print(all_tasks)
    print(all_optional_taks)
    print(all_workers)
    #print(model)
    print(model.comulatives)

    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    if status == cp_model.OPTIMAL:
        print('Optimal', solver.ObjectiveValue())
        for key in all_workers:
            print(workers[key], solver.Value(all_workers[key].start), solver.Value(all_workers[key].end), solver.Value(all_workers[key].duration))
        print('tasks')
        for key in all_tasks:
            print(tasks[key],solver.Value(all_tasks[key].start), solver.Value(all_tasks[key].end), solver.Value(all_tasks[key].duration))
        #
        print('option tasks')

        for key, items in mayperform.items():
            idx = tasks.index(key)
            for item in items:
                group_idx = groups.index(item)
                print(tasks[idx], groups[group_idx], solver.Value(all_optional_taks[idx][group_idx].present), 'Duration = ', solver.Value(all_optional_taks[idx][group_idx].duration))
        #model.show_cum(solver)
create_model()