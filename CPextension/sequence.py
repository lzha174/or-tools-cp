import collections

from ortools.sat.python import cp_model
# in this file, we implment sequence function
# so we can constrint start, finish and extra features for a sequence
# use a hamliton path to find ranks to determine the sequence

def no_optional():
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
    presences_per_machines = collections.defaultdict(list)
    ranks_per_machines = collections.defaultdict(list)
    starts_per_machines = collections.defaultdict(list)
    ends_per_machines = collections.defaultdict(list)
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
            presences_per_machines[m].append(True)
            intervals_per_jobs[j].append((start_var, end_var))
            starts_per_machines[m].append(start_var)
            ends_per_machines[m].append(end_var)

            l_rank = model.NewIntVar(-1, nbJobs, 'rank' + suffix)
            ranks_per_machines[m].append(l_rank)
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
    # find ranking on each machine
    for m in Mchs:
        machine_ranks = ranks_per_machines[m]
        machine_starts = starts_per_machines[m]
        machine_ends = ends_per_machines[m]
        num_machine_tasks = len(machine_starts)
        all_machine_tasks = range(num_machine_tasks)
        print('nb jobs', nbJobs)
        arcs = []
        # add a dumy node
        starts = []
        for i in all_machine_tasks:
            start_lit = model.NewBoolVar('')
            starts.append(start_lit)
            arcs.append([0, i+1, start_lit])
            # If this task is the first, set both rank and start to 0.
            # the rank var for this task is zero if this task is the starting one
            model.Add(machine_ranks[i] == 0).OnlyEnforceIf(start_lit)
            # Final arc from an arc to the dummy node.
            end_lit = model.NewBoolVar('')
            arcs.append([i + 1, 0, end_lit])
            for j in all_machine_tasks:
                if i == j:
                    continue

                # add links between all optional tasks, both directions

                lit = model.NewBoolVar('%i follows %i' % (j, i))
                arcs.append([i + 1, j + 1, lit])
                model.Add(machine_starts[j] > machine_ends[i]).OnlyEnforceIf(lit)
                # Maintain rank incrementally.
                # the ranking also follows
                model.Add(machine_ranks[j] == machine_ranks[i] + 1).OnlyEnforceIf(lit)
        if arcs:
            print(arcs)
                # A circuit is a unique Hamiltonian path in a subgraph of the total graph
            model.AddCircuit(arcs)
        #model.AddBoolXOr(starts)
    # implement same sequence contarint, the mapped intervals on two sequences should have same order

    # Objective.
    for m in range(0,nbMchs - 1):
        for m1 in range(m+1, nbMchs):
            rank_tasks = ranks_per_machines[m]
            rank1_tasks = ranks_per_machines[m1]
            num_machine_tasks = len(machine_starts)
            all_machine_tasks = range(num_machine_tasks)
            for t in all_machine_tasks:
                model.Add(rank_tasks[t] == rank1_tasks[t])

    objective_var = model.NewIntVar(0, 9999, 'obj')
    model.AddMaxEquality(objective_var, ends_per_jobs)
    model.Minimize(objective_var)
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 1
    print('solve', cp_model.FEASIBLE)
    status = solver.Solve(model)
    print(status,  cp_model.FEASIBLE)
    if status == cp_model.OPTIMAL or  status == cp_model.FEASIBLE:
        print(solver.Value(objective_var))
        for m in Mchs:
            ranks = {}
            for i in Jobs:
                r = int(solver.Value(ranks_per_machines[m][i]))
                ranks[r] = i
                print(r'rank for job %i is %i on machine %i' % (i, solver.Value(ranks_per_machines[m][i]), m ))
            od = collections.OrderedDict(sorted(ranks.items()))
            print(od)
