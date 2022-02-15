
from ortools.sat.python import cp_model

from initial_paras import *
from intervals import IntInterval
# This model require three input files.
# file 1: staff.csv defining station demand for each day,shift, station. This data is obtained from heuristic model output after a local
# file 2: worker skillset defining worker eligibility for each station. This data is extractd from database by checking who is doing what between certain dates
# file 3: worker availablity defiing worker availability for a period. This data is extracted from database by checking if this worker is at a station at a particular day
# to handle insufficient staff at a station of a day/shift, we add fake users with high cost
# to change planning horizon, change nbDays. The humber can't be more than the max day index from staff data.
def row_process(row, worker_profile):
    idx = paras[name_to_idx_user_str][row.user]
    skillset = []
    if row.acc == 1:
        skillset.append(paras[stage_to_idx_str]['acc'])
    if row.gross == 1:
        skillset.append(paras[stage_to_idx_str]['gross'])
    if row.process == 1:
        skillset.append(paras[stage_to_idx_str]['process'])
    if row.section == 1:
        skillset.append(paras[stage_to_idx_str]['section'])
    if row.signout == 1:
        skillset.append(paras[stage_to_idx_str]['signout'])
    worker_profile[idx] = worker_profile_type(idx=idx, skillset=skillset)


def station_row_process(row, station_demand):
    day = row.day
    shift = row.period
    stage = row.stage
    value = row.value
    #print('day = ', day, 'shift=', shift)
    station_demand[day, shift, stage] = value

def worker_row_process(row, worker_available):
    user = row.user
    day = row.day
    if user not in worker_available:
        worker_available[user] = []
    worker_available[user].append(day)

# Load worker availablity
def load_worker_avaliable():
    df = pd.read_csv('worker_available.csv')
    #df = load_from_storage('worker_aval')
    worker_available = {}
    df.apply(worker_row_process, args=(worker_available,), axis = 1)

    for i in range(nb_fake_users):
        fake_user_name =fake_user_suffix+f'{i}'
        worker_available[fake_user_name] = [i for i in range(20)]

    print(worker_available)
    paras['woker_available'] = worker_available

# load station demand for each day, shift
def load_station_demand():
    df = pd.read_csv("staff.csv")
    #df = load_from_storage("station_demand")
    station_demand = {}
    df.apply(station_row_process, args=(station_demand,), axis = 1)
    print(df[df.stage == 4])
    #for key in station_demand:
        #print('key = ', key, 'value =', station_demand[key])
    return station_demand

# Load skillset for each worker
def load_profile(worker_profile = {}):
    df = pd.read_csv("worker_profile.csv")
    #df = load_from_storage('worker_profile')
    columns = df.columns.tolist()
    columns.remove('user')
    print(columns)

    unused_users = ['ExternalInterface', 'system', 'Unknown']
    df = df[~df.user.isin(unused_users)]

    df['total'] = df.sum(axis=1, numeric_only=True)

    # do not include staff who dont have skillset at all during the planned period
    df = df[df.total > 0]
    users = df['user'].unique().tolist()
    print(users)
    # need a bunch of fake users to handle feasibility with high cost
    fake_users = [fake_user_suffix+f'{i}' for i in range(nb_fake_users)]
    users = users + fake_users
    print(df)
    # make a map
    name_to_idx_user = {name: idx for idx, name in enumerate(users)}
    idx_to_name_user = {idx: name for idx, name in enumerate(users)}
    paras[name_to_idx_user_str] = name_to_idx_user
    paras[idx_to_name_usr_str] = idx_to_name_user

    stage_to_idx =  {stage: idx for idx, stage in enumerate(columns)}
    idx_to_stage = {idx: stage for idx, stage in enumerate(columns)}
    paras[stage_to_idx_str] = stage_to_idx
    paras[idx_to_stage_str] = idx_to_stage
    print(stage_to_idx)
    print(idx_to_stage)

    df.apply(row_process, args=(worker_profile,), axis=1)

    # append fake users to worker profile
    for i in range(nb_fake_users):
        fake_user_idx = paras[name_to_idx_user_str][fake_user_suffix+f'{i}']
        paras['fake_users'].append(fake_user_idx)
        worker_profile[fake_user_idx] = worker_profile_type(idx=fake_user_idx, skillset=[0,1,2,3,4])

    return df

# note: when uploading to notebook, copy from here, Above code should use the note book at the moment
def model():
    # first define shifts, each shift has 4 work stations, each station has a demand
    # need a way to mark the shift is for which day during a week
    paras['fake_users'] = []
    worker_profile = {}
    load_profile(worker_profile)

    #stage_names = {0: 'accession', 1: 'gross', 2:'process', 4:'section', 5: 'signout'}

    station_demand = load_station_demand()

    load_worker_avaliable()

    # note: when on notebook, this can go wrong. need to check it later
    nbDays = 5
    day_intervals = IntInterval.closed_open(0, nbDays)
    # just read in the staffing for now
    stages = paras[idx_to_stage_str].keys()

    shift_staffing = {}
    for key, value in station_demand.items():
        day = key[0]
        if day not in day_intervals: continue

        shift_period = key[1]
        stage = key[2]
        demand = value
        shift_staffing[day, shift_period, stage] = demand
    # define workers
    # each worker has a certain skillset?, each worker is avaliable on certain days? each worker can do no more than 2 shifts a day, they must be contineious?
    roster_type = collections.namedtuple('roster', 'skillset avaliable cost real_user')
    nbStaff = len(worker_profile)
    rosterings_paras = {}
    for s in range(nbStaff):
        staff_name = paras[idx_to_name_usr_str][s]
        isRealUser = s not in paras['fake_users']
        profile = worker_profile[s]
        avaliable = paras['woker_available'][staff_name]

        if staff_name == 'ArionM':
            print('ArionM', avaliable)

        cost = {}
        for day in range(nbDays):
            for shift in range(nb_shifts):
                # make shift 0 attractive for worker 1
                if s == 1 and shift == 0:
                    c = 5
                else:
                    c = 10
                cost[day, shift] = c
        # cost is define by day and shift
                if isRealUser == False:
                    cost[day, shift] = 1000
        rosterings_paras[s] = roster_type(skillset=profile.skillset, avaliable=avaliable, cost=cost, real_user = isRealUser)

    print(shift_staffing)
    print(rosterings_paras)

    # Creates the model.
    model = cp_model.CpModel()
    # need to know which staff is able to do each stage for every day
    rostering = {}
    worker_eligible_each_slot = {}
    worker_eligible_stages_each_shift_period = {}
    worker_all_elegible_slots = {}
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
                        worker_eligible_each_slot[day, shift_period, stage].append(
                            rostering[w, day, shift_period, stage])

                        key = worker_eligible_stages_each_shift_period.get((w, day, shift_period), None)
                        if key is None:
                            worker_eligible_stages_each_shift_period[w, day, shift_period] = []
                        worker_eligible_stages_each_shift_period[w, day, shift_period].append(
                            rostering[w, day, shift_period, stage])

                        if rosterings_paras[w].real_user:
                            key = worker_all_elegible_slots.get((w, day), None)
                            if key is None:
                                worker_all_elegible_slots[w, day] = []
                            worker_all_elegible_slots[w, day].append(rostering[w, day, shift_period, stage])


    # constraint 1, for each slot (day, shift_period, stage), deamnd needs to be satisifed
    for day in range(nbDays):
        for shift_period in range(nb_shifts):
            for stage in stages:
                demand = shift_staffing[day, shift_period, stage]
                print(f'day{day}, shift {shift_period}, stage_demand{stage, demand}')
                print(worker_eligible_each_slot[day, shift_period, stage])
                model.Add(sum(value for value in worker_eligible_each_slot[day, shift_period, stage]) >= demand)

    # constriant 2: a worker can only do one stage at each shift period
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
        model.Add(sum(s for s in worker_eligible_stages_each_shift_period[key]) == worker_is_working_shift_period[
            worker, day, shift_period])

    # constraint 3: a worker can only do consective slots each day? if he has to do more than one slot
    # allowed_pattern = [[0, 0, 0], [0, 0, 1], [0, 1, 0], [0, 1, 1], [1, 0, 0], [1, 1, 0]]
    forbidden_patterns = [[1, 0, 1], [1, 1, 1]]
    for w in range(nbStaff):
        if rosterings_paras[w].real_user == False: continue
        for day in range(nbDays):
            if day in rosterings_paras[w].avaliable:
                # do a rolling window of 3? no zero in the middle
                roll_window_left = 0
                # window_width = 3
                model.AddForbiddenAssignments([worker_is_working_shift_period[w, day, roll_window_left],
                                               worker_is_working_shift_period[w, day, roll_window_left + 1],
                                               worker_is_working_shift_period[w, day, roll_window_left + 2]],
                                              forbidden_patterns)
                # model.AddAllowedAssignments([worker_is_working_shift_period[w, day, roll_window_left],
                #                             worker_is_working_shift_period[w, day, roll_window_left + 1],
                #              worker_is_working_shift_period[w, day, roll_window_left + 2]], allowed_pattern)
    # constraint 4: this is a new constraint I am trying. Assume there are overlapped shifts, the overlapped part makes a new shift, if the overlapped shift is taken, one of the two shifts must be taken as well, but not both
    # therefore the forbidden partterns above still applies
    # for this example, assume two shifts are 8:00 - 1600, 12:00 - 18:00, there are three shifts 8:00 - 12,  12 - 16, 16 - 18. The overlapped part is 12-16
    # i can enforce worker at the same station from 8-12 and 12-16
    useOverlap = False
    if useOverlap:
        for w in range(nbStaff):
            for day in range(nbDays):
                model.Add(worker_is_working_shift_period[w, day, 0] + worker_is_working_shift_period[w, day, 2] <= 1).OnlyEnforceIf(worker_is_working_shift_period[w, day, 1])
                for stage in stages:
                    if stage in rosterings_paras[w].skillset and day in rosterings_paras[w].avaliable:
                        model.Add(rostering[w, day, 0, stage] == rostering[w, day, 1, stage]).OnlyEnforceIf(worker_is_working_shift_period[w, day, 0])
                        model.Add(rostering[w, day, 2, stage] == rostering[w, day, 1, stage]).OnlyEnforceIf(
                            worker_is_working_shift_period[w, day, 2])

    #constraint 5: find min number of total assigned shifts and max no. of total assigned shfits
    use_even_shifts = False
    if use_even_shifts:
        totalShifts = []
        for key, values in worker_all_elegible_slots.items():
            suffix = f'total_shift_{key}'
            totalShiftVar = model.NewIntVar(0, nb_shifts * nbDays, suffix)
            model.Add(totalShiftVar == sum(v for v in values ))
            totalShifts.append(totalShiftVar)

        minTotalShifts = model.NewIntVar(0, nb_shifts * nbDays,  'min_total')
        maxTotalShifts = model.NewIntVar(0, nb_shifts * nbDays, 'max_total')

        model.AddMinEquality(minTotalShifts, totalShifts)
        model.AddMaxEquality(maxTotalShifts, totalShifts)

    # constraint 6: every worker must work on the day he is avaliable
    meet_minimum_hours = True
    if meet_minimum_hours:
        for key, values in worker_all_elegible_slots.items():
            day = key[1]
            w = key[0]
            suffix = f'total_shift_{key}'
            totalShiftVar = model.NewIntVar(0, nb_shifts * nbDays, suffix)
            model.Add(totalShiftVar == sum(v for v in values ))
            model.Add(totalShiftVar >= 1)

    # objective minimise cost
    # how to assign 'evenly'?
    # I can find nb of shifts worked each day, ? I can find total nb of shifts worked over a week for each staff, make distrbution more even?
    # i can add a soft prefrence cost constriant? if an employee likes a particular shift period, make the cost smaller
    objective = 'min_cost'
    #objective = 'even_shift'
    if objective == 'min_cost':
        model.Minimize(
            sum(v * rosterings_paras[key[0]].cost[key[1], key[2]] for key, v in worker_is_working_shift_period.items()))
    # objective 2: minimise difference between max and min total number of assigned shifts to workers
    # get total of assigned shifts for each worker
    if objective == 'even_shift':
        model.Minimize(maxTotalShifts - minTotalShifts )
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
                    data = [day, shift_period, paras[idx_to_stage_str][stage]]

                    for w in range(nbStaff):
                        if day in rosterings_paras[w].avaliable and stage in rosterings_paras[w].skillset:
                            v = solver.Value(rostering[w, day, shift_period, stage])
                        else:
                            v = 0
                        data.append(v)
                    output.append(data)
        worker_strs = [paras[idx_to_name_usr_str][i] for i in range(nbStaff)]
        columns = ['day', 'shift', 'stage'] + worker_strs
        result_df = pd.DataFrame(output,
                                 columns=columns)
        result_df['Total'] = result_df.iloc[:, -nbStaff:-1].sum(axis=1)

        print(result_df['Total'])
        to_csv(result_df, 'assignment.csv')

        # i want to know each worker is doing what station at each shift
        output = []
        for w in range(nbStaff):
            for day in range(nbDays):
                data = [paras[idx_to_name_usr_str][w], day]
                for shift_period in range(nb_shifts):
                    value = 0
                    onStage = None
                    for stage in stages:
                        if day in rosterings_paras[w].avaliable and stage in rosterings_paras[w].skillset:
                            value = solver.Value(rostering[w, day, shift_period, stage])
                            if value:
                                onStage = stage
                                break

                    if value:
                            # this worker is at this station
                        data.append(paras[idx_to_stage_str][onStage])
                    else:
                        data.append('off')
                output.append(data)

        shift_strs = ['shift_' + str(s) for s in range(nb_shifts)]
        columns = ['worker', 'day'] + shift_strs
        result_df = pd.DataFrame(output, columns=columns)
        to_csv(result_df, 'station_worker.csv')

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

        output = [[paras[idx_to_name_usr_str][key[0]], key[1]] + value for key, value in shift_pattern_output.items()]
        shift_strs = ['shift_' + str(s) for s in range(nb_shifts)]
        columns = ['worker', 'day'] + shift_strs
        result_df = pd.DataFrame(output, columns=columns)
        to_csv(result_df, 'worker_shift.csv')
        temp = result_df[result_df.day == 0]
        print(temp)
        if objective == 'even_shift':
            print('max total shifts', solver.Value(maxTotalShifts))
            print('min total shifts', solver.Value(minTotalShifts))



model()
#load_station_demand()
#load_profile()
#load_worker_avaliable()