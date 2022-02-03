
from ortools.sat.python import cp_model
from commonstr import *

def model():
    # first define shifts, each shift has 4 work stations, each station has a demand
    # need a way to mark the shift is for which day during a week
    nbDays = 2
    # just read in the staffing for now
    stages = [0, 1, 3, 4]
    shift_staffing = {}
    for key, value in staffing.items():
        day = key[0]
        # plan schedule for 5 days
        if day == nbDays: break
        shift_period = key[1]
        for stage, demand in value.items():
            if stage not in stages: continue
            # hack
            demand = 1
            shift_staffing[day, shift_period, stage] = demand
    # define workers
    # each worker has a certain skillset?, each worker is avaliable on certain days? each worker can do no more than 2 shifts a day, they must be contineious?
    roster_type = collections.namedtuple('roster', 'skillset avaliable cost')
    nbStaff = 8
    rosterings_paras = {}
    for s in range(nbStaff):
        avaliable = [0, 1 ,2, 3, 4]
        cost = {}
        for day in range(nbDays):
            for shift in range(nb_shifts):
                # make shift 0 attractive for worker 1
                if s == 1 and shift == 0: c = 5
                else:
                    c = 10
                cost[day, shift] = c

        rosterings_paras[s] = roster_type(skillset=[0, 1, 3, 4], avaliable = avaliable, cost = cost)

    print(shift_staffing)
    print(rosterings_paras)

    # Creates the model.
    model = cp_model.CpModel()
    # need to know which staff is able to do each stage for every day
    rostering = {}
    worker_eligible_each_slot = {}
    worker_eligible_stages_each_shift_period = {}
    for w in range(nbStaff):
        for day in range(nbDays):
            for shift_period in range(nb_shifts):
                for stage in stages:
                    # add a bool var if this worker is avaliable and able to do this stage
                    if stage in rosterings_paras[w].skillset and day in rosterings_paras[w].avaliable:
                        suffix = f'w{w}_d{day}_s{shift_period}_stage{stage}'
                        rostering[w, day, shift_period, stage] = model.NewBoolVar(suffix)
                        # append this variable to the slot dict
                        key = worker_eligible_each_slot.get((day, shift_period, stage), None)
                        if key is None:
                            worker_eligible_each_slot[day, shift_period, stage] = []
                        # store this staff in eligible slot array for the deamnd constraint
                        worker_eligible_each_slot[day, shift_period, stage].append(rostering[w, day, shift_period, stage])

                        key = worker_eligible_stages_each_shift_period.get((w, day, shift_period), None)
                        if key is None:
                            worker_eligible_stages_each_shift_period[w, day, shift_period] = []
                        worker_eligible_stages_each_shift_period[w, day, shift_period].append(rostering[w, day, shift_period, stage])

    #constraint 1, for each slot (day, shift_period, stage), deamnd needs to be satisifed
    for day in range(nbDays):
        for shift_period in range(nb_shifts):
            for stage in stages:
                demand = shift_staffing[day, shift_period, stage]
                print(f' day{day}, shift {shift_period}, stage_demand{stage, demand}')
                print(worker_eligible_each_slot[day, shift_period, stage])
                model.Add(sum(value  for value in worker_eligible_each_slot[day, shift_period, stage]) >= demand)



    #constriant 3: a worker can only do one stage at each shift period
    # need to know each shift period, the worker can do which stages
    worker_is_working_shift_period = {}
    for key in worker_eligible_stages_each_shift_period:
        print('a worker at most one stage per shfit period')
        print(f'w{key[0]}, day{key[1]}, shift {key[2]}')
        worker = key[0]
        day = key[1]
        shift_period = key[2]
        suffix = f'w{key[0]}_day{key[1]}_shift {key[2]}_working'
        worker_is_working_shift_period[worker, day, shift_period] = model.NewBoolVar(suffix)

        print(worker_eligible_stages_each_shift_period[key])
        # if a worker is working this shift, one stage must be chosen
        model.Add(sum(s for s in worker_eligible_stages_each_shift_period[key]) == worker_is_working_shift_period[worker, day, shift_period])

    # constraint 2: a worker can only do consective slots each day? if he has to do more than one slot
    allowed_pattern = [[0, 0, 0], [0, 0, 1], [0, 1, 0], [0, 1, 1], [1, 0, 0], [1, 1, 0]]
    for w in range(nbStaff):
        for day in range(nbDays):
            if day in rosterings_paras[w].avaliable:
                # do a rolling window of 3? no zero in the middle
                roll_window_left = 0
                window_width = 3
                model.AddAllowedAssignments([worker_is_working_shift_period[w, day, roll_window_left],
                                             worker_is_working_shift_period[w, day, roll_window_left + 1],
                              worker_is_working_shift_period[w, day, roll_window_left + 2]], allowed_pattern)
    # objective minimise cost
    # how to assign 'evenly'?
    # I can find nb of shifts worked each day, ? I can find total nb of shifts worked over a week for each staff, make distrbution more even?
    # i can add a soft prefrence cost constriant? if an employee likes a particular shift period, make the cost smaller

    model.Minimize(sum(v * rosterings_paras[key[0]].cost[key[1], key[2]] for key, v in worker_is_working_shift_period.items()))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = paras['max_serach_time_sec']
    status = solver.Solve(model)
    print(status)
    assert (status == cp_model.FEASIBLE or status == cp_model.OPTIMAL)
    if status == cp_model.FEASIBLE or status == cp_model.OPTIMAL:
        print(status)
        output = []
        # i want to know each slot is done by which workers
        for day in range(nbDays):
            for shift_period in range(nb_shifts):
                for stage in stages:
                    data = [day, shift_period, stage]

                    for w in range(nbStaff):
                        if day in rosterings_paras[w].avaliable:
                            v = solver.Value(rostering[w, day, shift_period, stage])
                        else:
                            v = 0
                        data.append(v)
                    output.append(data)
        worker_strs = ['worker_' + str(i) for i in range(nbStaff)]
        columns = ['day', 'shift', 'stage'] + worker_strs
        result_df = pd.DataFrame(output,
                                 columns=columns)
        to_csv(result_df,'assignment.csv')

        # i want to know for each worker the shift pattern
        shift_pattern_output = {}
        for key, value in worker_is_working_shift_period.items():
            worker = key[0]
            day = key[1]
            shift = key[2]
            v = solver.Value(value)
            worker_key = shift_pattern_output.get((worker, day), None)
            if worker_key is None:
                shift_pattern_output[worker, day] = []
            shift_pattern_output[worker, day].append(v)

        output = [[key[0], key[1]] + value for key, value in shift_pattern_output.items()]
        shift_strs = ['shift_' + str(s) for s in range(nb_shifts)]
        columns = ['worker','day'] + shift_strs
        result_df = pd.DataFrame(output, columns = columns)
        to_csv(result_df, 'worker_shift.csv')
        temp = result_df[result_df.day == 0]
        print(temp)





model()