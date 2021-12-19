import collections

from ortools.sat.python import cp_model


def create_model():

    # duration for pairs of disciplines that require break if they follow each other
    BreakDuration = 1
    # each day has 6 time units
    DayDuration = 6
    # 4 days to plan each period
    NumberOfDaysPerPeriod = 4
    # discipline that must occur in the morning
    MorningDiscipline = ["Sport"]
    # pairs of disciplines that require breaks
    NeedBreak = [
        ("Maths", "Physics"),
        ("Biology", "Physics"),
        ("Economy", "Biology"),
        ("Geography", "Economy"),
        ("History", "Geography")
    ]
    # Teachers skill set. Note that Marie-Laure can teach both Geography and History
    TeacherDisciplineSet = [
        ("Jean-Francois", "Maths"),
        ("Pierre", "French"),
        ("Odile", "Economy"),
        ("Paul", "Physics"),
        ("Veronique", "Biology"),
        ("Patrick", "Sport"),
        ("Jerome", "History"),
        ("Marie-Laure", "Geography"),
        ("Marie-Laure", "History"),
    ]
    # Each tuple is (class, discipline, duration, repeat)
    RequirementSet = [

        ("X", "French", 1, 3),
        ("X", "Maths", 2, 4),
        ("X", "Physics", 1, 2),

        ("Y", "French", 1, 3),
        ("Y", "Maths", 1, 2),
        ("Y", "Physics", 1, 1),

        ("Z", "French", 2, 4),
        ("Z", "Maths", 1, 2),
        ("Z", "Physics", 1, 1)]

    DedicatedRoomSet = [("Stadium", "Sport"),
                        ("Lab", "Biology")
                        ]

    Room = ["R1", "R2", "R3", "R4", "R5", "R6", "Stadium", "Lab"]

    # get all classes from requirements
    Class = set()
    for r in RequirementSet:
        Class.add(r[0])
    Class = sorted(list(Class))

    # get all teachers
    Teachers = set()
    for t in TeacherDisciplineSet:
        Teachers.add(t[0])
    Teachers = sorted(list(Teachers))

    # get all displines
    Disciplines = set()
    for t in TeacherDisciplineSet:
        Disciplines.add(t[1])
    Disciplines = sorted(list(Disciplines))

    HalfDayDuration = int(DayDuration / 2)
    MaxTime = DayDuration * NumberOfDaysPerPeriod - 1
    RoomMaxIndex = len(Room) - 1

    print('Clases:', Class)
    print('Teachers:', Teachers)
    print('Disciplines:', Disciplines)
    print('half day duration = ', HalfDayDuration)
    print('max time=',MaxTime)
    # create compatibility of room/disciplne
    PossibleRooms = {}
    for d in Disciplines:
        for x in Room:
            # first check if <x,d> is in dedicated room set
            a = (x, d) in DedicatedRoomSet
            s = set()
            for k in Disciplines:
                if k == d: continue
                # room x is used for disciplne k
                if (x, k) in DedicatedRoomSet: s.add((x, k))
                # disciplne d is taught in some other room
            for z in Room:
                if (z, d) in DedicatedRoomSet: s.add((x, k))
            b = (len(s) == 0)
            result = a or b
            PossibleRooms[d, x] = result

    PossibleRoomIds = {}
    for d in Disciplines:
        s = set()
        for x in Room:
            if PossibleRooms[d, x] == True:
                s.add(Room.index(x))
        PossibleRoomIds[d] = s
    print('possible rooms', PossibleRoomIds)

    PossibleTeacherDiscipline = {}
    for x in Teachers:
        s = set()
        for d in Disciplines:
            if (x, d) in TeacherDisciplineSet:
                s.add(d)
        PossibleTeacherDiscipline[x] = s

    print('Possible Teacher Discipline:', PossibleTeacherDiscipline)

    PossibleTeacherIds = {}
    for d in Disciplines:
        s = set()
        for z in Teachers:
            if d in PossibleTeacherDiscipline[z]:
                s.add(Teachers.index(z))
        PossibleTeacherIds[d] = s
    print('Possible teacher id', PossibleTeacherIds)

    TeacherUpperBound = len(Teachers) - 1

    Instance = collections.namedtuple('instance_data', 'cls discipline Duration repetition id requirementId')
    InstanceSet = []
    for z, r in enumerate(RequirementSet):
        for i in range(r[3]):
            InstanceSet.append(
                Instance(cls=r[0], discipline=r[1], Duration=r[2], repetition=r[3], id=i, requirementId=z))
    for i in InstanceSet:
        print(i)
    # create decision varaibles
    model = cp_model.CpModel()
    Starts = {}
    Ends = {}
    Rooms = {}
    Teachers_vars = {}
    for i in InstanceSet:
        start_var = model.NewIntVar(0, MaxTime, name='start of instance %i' % (InstanceSet.index(i)))
        Starts[i] = start_var

        end_var = model.NewIntVar(0, 77, name='end of instance %i' % (InstanceSet.index(i)))
        Ends[i] = end_var

        room_var = model.NewIntVar(0, RoomMaxIndex, name='room of instance %i' % (InstanceSet.index(i)))
        Rooms[i] = room_var

        teacher_var = model.NewIntVar(0, TeacherUpperBound, name='teacher of instance %i' % (InstanceSet.index(i)))
        Teachers_vars[i] = teacher_var

    classTeacher = {}
    for c in Class:
        for d in Disciplines:
            c_d_var = model.NewIntVar(0, TeacherUpperBound, name='class %i of d %i' % (Class.index(c), Disciplines.index(d)))
            classTeacher[c, d] = c_d_var

    makespan = model.NewIntVar(0, MaxTime, name='makespan')
    model.AddMaxEquality(makespan, Ends.values())

    durations = [0] * len(Class)
    for i in InstanceSet:
        # get sum of duration for each class
        durations[Class.index(i.cls)] = durations[Class.index(i.cls)] + i.Duration
    print(durations)
    maxDUration = max(durations)
    model.Add(makespan >= maxDUration)

    # ensure the course ends after it starts
    for i in InstanceSet:
        model.Add(Ends[i] == Starts[i] + i.Duration)
    # // ensure course numerotation is chronological
    for i in InstanceSet:
        for j in InstanceSet:
            if i.id < j.id and i.requirementId == j.requirementId:
                model.Add(Starts[i] < Starts[j])

    # ensure that a teacher is required once at any time point
    includeInstanceForTeacher = {}
    for idx, r in enumerate(InstanceSet):
        for x in Teachers:
            if r.discipline in PossibleTeacherDiscipline[x]:

                includeFLags = []
                for idx1, o in enumerate(InstanceSet):
                    suffix = 't-r%i x%s o%i' % (idx, x, idx1)
                    includeFLags.append(model.NewIntVar(0, 1, suffix))

                includeInstanceForTeacher[idx, x] = includeFLags
                for o in InstanceSet:
                    index = InstanceSet.index(o)
                    # instance o is not in the possible displine of teacher x, dont include this instance
                    if o.discipline not in PossibleTeacherDiscipline[x]:
                        model.Add(includeFLags[index] == False)
                        continue

                    startAfterR = model.NewBoolVar('start_% i after r' % (index))

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(startAfterR)
                    model.Add(Starts[o] < Starts[r]).OnlyEnforceIf(startAfterR.Not())

                    startBeforeEndR = model.NewBoolVar('start %i before end r' % (index))

                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(startBeforeEndR)
                    model.Add(Starts[o] >= Ends[r]).OnlyEnforceIf(startBeforeEndR.Not())

                    teacherX = model.NewBoolVar('same teacher %i for r' % (index))
                    model.Add(Teachers_vars[o] == Teachers.index(x)).OnlyEnforceIf(teacherX)
                    model.Add(Teachers_vars[o] != Teachers.index(x)).OnlyEnforceIf(teacherX.Not())

                    # if all three bools are true, include this o in the sum
                    model.Add(startAfterR + startBeforeEndR + teacherX == 3).OnlyEnforceIf(includeFLags[index])
                    model.Add(startAfterR + startBeforeEndR + teacherX < 3).OnlyEnforceIf(includeFLags[index].Not())
                model.Add(sum(includeInstanceForTeacher[idx, x][InstanceSet.index(o)] for o in InstanceSet) < 2)

    #  // ensure that a room is required once at any time point.
    includesFLagsRoomDict = {}
    for idx, r in enumerate(InstanceSet):

        for x in Room:
            if PossibleRooms[r.discipline, x] == 1:
                includesRoom = []
                for idx1, o in enumerate(InstanceSet):
                    suffix = 'r%i x%s o%i' % (idx, x, idx1)
                    includesRoom.append(model.NewBoolVar(suffix))
                includesFLagsRoomDict[idx, x] = includesRoom

                for o in InstanceSet:
                    index = InstanceSet.index(o)
                    if PossibleRooms[o.discipline, x] == 0:
                        model.Add(includesRoom[index] == False)
                        continue

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(includesRoom[index])
                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(includesRoom[index])

                    startAfterR = model.NewBoolVar('start_% i after %i' % (index, InstanceSet.index(r)))

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(startAfterR)
                    model.Add(Starts[o] < Starts[r]).OnlyEnforceIf(startAfterR.Not())

                    startBeforeEndR = model.NewBoolVar('start %i before end %i' % (index, InstanceSet.index(r)))

                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(startBeforeEndR)
                    model.Add(Starts[o] >= Ends[r]).OnlyEnforceIf(startBeforeEndR.Not())

                    sameRoom = model.NewBoolVar('same room %i for r' % (index))
                    model.Add(Rooms[o] == Room.index(x)).OnlyEnforceIf(sameRoom)
                    model.Add(Rooms[o] != Room.index(x)).OnlyEnforceIf(sameRoom.Not())

                    # if all three bools are true, include this o in the sum
                    model.Add(startAfterR + startBeforeEndR + sameRoom == 3).OnlyEnforceIf(includesRoom[index])

                    model.Add(startAfterR + startBeforeEndR + sameRoom < 3).OnlyEnforceIf(includesRoom[index].Not())

                model.Add(sum(includesFLagsRoomDict[idx, x][i] for i, v in enumerate(InstanceSet)) < 2)

    #  // ensure that a class follows one course at a time
    sameCLassInstanceFLagsDict = {}
    for x in Class:
        for idx, r in enumerate(InstanceSet):

            if r.cls == x:
                sameCLassInstanceFLags = []
                for idx1, o in enumerate(InstanceSet):
                    suffix = 'c-r%i x%s o%i' % (idx, x, idx1)
                    sameCLassInstanceFLags.append(model.NewIntVar(0, 1, suffix))
                sameCLassInstanceFLagsDict[idx, x] = sameCLassInstanceFLags

                for o in InstanceSet:
                    index = InstanceSet.index(o)
                    if o.cls != x:
                        model.Add(sameCLassInstanceFLags[index] == False)
                        continue
                    classStartAfterR = model.NewBoolVar('start %i after r' % index)
                    classStartBeforeEndR = model.NewBoolVar('start % i b4 end r' % index)

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(classStartAfterR)
                    model.Add(Starts[o] < Starts[r]).OnlyEnforceIf(classStartAfterR.Not())

                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(classStartBeforeEndR)
                    model.Add(Starts[o] >= Ends[r]).OnlyEnforceIf(classStartBeforeEndR.Not())

                    model.Add(classStartAfterR + classStartBeforeEndR == 2).OnlyEnforceIf(sameCLassInstanceFLags[index])
                    model.Add(classStartAfterR + classStartBeforeEndR < 2).OnlyEnforceIf(
                        sameCLassInstanceFLags[index].Not())

                sameCLassInstanceFLagsDict[x, idx] = sameCLassInstanceFLags
                # print(InstanceSet.index[o]for o in InstanceSet)
                model.Add(sum(sameCLassInstanceFLagsDict[x, idx][InstanceSet.index(o)] for o in InstanceSet) < 2)

    # ensure the teacher can teach the discipline
    for r in InstanceSet:
        v = []
        for i in PossibleTeacherIds[r.discipline]:
            v.append([i])
        model.AddAllowedAssignments([Teachers_vars[r]], v)


    # ensure the room can support the discipline
    for r in InstanceSet:
        print([r.discipline], [[x] for x in PossibleRoomIds[r.discipline]])
        v = [[x] for x in PossibleRoomIds[r.discipline]]
        model.AddAllowedAssignments([Rooms[r]],v)


    # ensure that for given class and discipline, the teacher is always the same
    for x in Class:
        for d in Disciplines:
            for r in InstanceSet:
                if r.cls == x and r.discipline == d:
                    model.Add(Teachers_vars[r] == classTeacher[x, d])

    # ensure a course starts and end the same half-day
    for idx, v in enumerate(InstanceSet):
        if v.Duration > 1:
            same_day_start = model.NewIntVar(0, MaxTime, 'same_day_s%i' % idx)
            same_day_end = model.NewIntVar(0, MaxTime, 'same_day_e%i' % idx)
            end_var_minus = model.NewIntVar(0, MaxTime, 'end_var_minus%i' % idx)
            model.Add(end_var_minus == Ends[v] - 1)
            model.AddDivisionEquality(same_day_start, Starts[v], HalfDayDuration)
            model.AddDivisionEquality(same_day_end, end_var_minus, HalfDayDuration)
            model.Add(same_day_end == same_day_start)
    #  insert break duration between specified disciplines
    for idx, i in enumerate(InstanceSet):
        for idx1, j in enumerate(InstanceSet):

            if idx1 <= idx: continue
            if i.cls == j.cls and (i.discipline, j.discipline) in NeedBreak or (
            j.discipline, i.discipline) in NeedBreak:
                print(i.discipline, j.discipline)
                # course do not belong to the same day
                differnt_day = model.NewBoolVar('%i-s-d-%i' % (idx, idx1))
                day_i = model.NewIntVar(0, MaxTime, 'day_%i' % (idx))
                day_j = model.NewIntVar(0, MaxTime, 'day_%i' % (idx1))
                model.AddDivisionEquality(day_i, Starts[i], DayDuration)
                model.AddDivisionEquality(day_j, Starts[j], DayDuration)
                model.Add(day_i != day_j).OnlyEnforceIf(differnt_day)

                # // courses do not belong to the same half day
                same_day = model.NewBoolVar('%i-d-d-%i' % (idx, idx1))
                half_day_i = model.NewIntVar(0, MaxTime, 's-day_%i' % (idx))
                half_day_j = model.NewIntVar(0, MaxTime, 's-day_%i' % (idx1))
                model.AddDivisionEquality(half_day_i, Starts[i], HalfDayDuration)
                model.AddDivisionEquality(half_day_j, Starts[j], HalfDayDuration)
                model.Add(half_day_i != half_day_j).OnlyEnforceIf(same_day)

                # / courses are separated by BreakDuration
                same_half_day = model.NewBoolVar('%i-h-d-%i' % (idx, idx1))
                i_b4_j = model.NewBoolVar('%i_b4_%i' % (idx, idx1))
                model.Add(Ends[i] - Starts[j] >= BreakDuration).OnlyEnforceIf([i_b4_j, same_half_day])
                model.Add(Ends[j] - Starts[i] >= BreakDuration).OnlyEnforceIf([i_b4_j.Not(), same_half_day])

                model.AddBoolOr([differnt_day, same_day, same_half_day])

            # // avoid to have the same discipline taught twice a day
            if i.cls == j.cls and i.discipline == j.discipline:
                day_i = model.NewIntVar(0, MaxTime, 'same_displine_day_%i' % (idx))
                day_j = model.NewIntVar(0, MaxTime, 'same_displine_day_%i' % (idx1))
                model.AddDivisionEquality(day_i, Starts[i], DayDuration)
                model.AddDivisionEquality(day_j, Starts[j], DayDuration)
                model.Add(day_i != day_j)

    model.Minimize(makespan)
    solver = cp_model.CpSolver()
    # solver.parameters.max_time_in_seconds = 20
    status = solver.Solve(model)
    if status == cp_model.OPTIMAL:
        roomsClasses = {}
        for x in Room:
            roomsClasses[x] = []
        for i in InstanceSet:
            print(i.id, i.discipline, i.Duration, 'start = ', solver.Value(Starts[i]), 'end = ', solver.Value(Ends[i]))
        for i in InstanceSet:
            room = Room[solver.Value(Rooms[i])]
            roomsClasses[room].append((solver.Value(Starts[i]), solver.Value(Ends[i])))
        for key, value in roomsClasses.items():
            print(key, value)
        print('max make span = ', solver.ObjectiveValue())

create_model()
