import collections

from ortools.sat.python import cp_model

def create_model():

    BreakDuration = 1
    DayDuration = 6

    MorningDiscipline = ["Sport"]

    NeedBreak = [
                    ( "Maths", "Physics" ),
        ("Biology", "Physics" ),
        ("Economy", "Biology" ),
        ( "Geography", "Economy" ),
        ("History", "Geography")
    ]

    TeacherDisciplineSet = [
                           ("Jean-Francois", "Maths" ),
    ("Pierre", "French" ),
    ("Odile", "Economy" ),
    ("Paul", "Physics" ),
    ("Veronique", "Biology" ),
    ("Patrick", "Sport" ),
    ("Jerome", "History" ),
    ("Marie-Laure", "Geography" ),
    ("Marie-Laure", "History" ),
    ]

    NumberOfDaysPerPeriod = 4

    RequirementSet = [

                     ( "X", "French", 1, 3 ),
    ( "X", "Maths", 2, 4 ),
    ( "X", "Physics", 1, 2 ),

    ( "Y", "French", 1, 3 ),
    ( "Y", "Maths", 1, 2 ),
    ( "Y", "Physics", 1, 1 ),

    ( "Z", "French", 2, 4 ),
    ( "Z", "Maths", 1, 2 ),
    ( "Z", "Physics", 1, 1 )]

    #RequirementSet = [("X", "French", 1, 1), ( "Y", "French", 1, 1 )]

    DedicatedRoomSet = [("Stadium", "Sport" ),
    ("Lab", "Biology" )
    ]

    Room = ["R1", "R2", "R3", "R4", "R5", "R6", "Stadium", "Lab"]

    print(Room)

    # get all classes from requirements
    Class = set()
    for r in RequirementSet:
        Class.add(r[0])
    Class = sorted(Class)
    Class = list(Class)
    print(Class)

    # get all teachers
    Teacher = set()
    for t in TeacherDisciplineSet:
        Teacher.add(t[0])
    Teacher = list(Teacher)
    Teacher = sorted(Teacher)
    print(Teacher)
    # get all displines
    Discipline = set()
    for t in TeacherDisciplineSet:
        Discipline.add(t[1])
    print(Discipline)
    Discipline = list(Discipline)

    HalfDayDuration = int(DayDuration /2)
    MaxTime = DayDuration * NumberOfDaysPerPeriod
    print('max time = ', MaxTime)
    Time = range(MaxTime)

    # create copatability dic of possible rooms key is displine, room
    PossibleRoom = {}
    for d in Discipline:
        for x in Room:
            # first check if <x,d> is in dedicated room set
            a = (x,d) in DedicatedRoomSet
            #print(x, d, a)
            # find set of x  used for other displines
            s = set()
            for k in Discipline:
                if k == d: continue
                # room x is used for displine k
                if (x, k) in DedicatedRoomSet: s.add((x,k))
                # dispiline d is taught in some other room
            for z in Room:
                if (z, d) in DedicatedRoomSet: s.add((x,k))
            b = (len(s) == 0)
            result = a or b
            PossibleRoom[d, x] = result
    for key in PossibleRoom:
        print(key, PossibleRoom[key])
    NbRoom = len(Room) - 1
    print(NbRoom)

    RoomId = range(NbRoom)
    PossibleRoomIds = {}
    for d in Discipline:
        s = set()
        for x in Room:
            if PossibleRoom[d,x] == True:
                s.add(Room.index(x))
        PossibleRoomIds[d] = s
    print('possible rooms', PossibleRoomIds)

    PossibleTeacherDiscipline = {}
    for x in Teacher:
        s = set()
        for d in Discipline:
            if (x, d) in TeacherDisciplineSet:
                s.add(d)
        PossibleTeacherDiscipline[x] = s
    NbTeacher = len(Teacher) - 1
    print(NbTeacher)
    TeacherId = range(NbTeacher)

    print(PossibleTeacherDiscipline)

    PossibleTeacherIds = {}
    for d in Discipline:
        s = set()
        for z in Teacher:
            if d in PossibleTeacherDiscipline[z]:
                s.add(Teacher.index(z))
        PossibleTeacherIds[d] = s
    print(Teacher)
    print('possible teacher id',PossibleTeacherIds)

    Instance = collections.namedtuple('instance_data', 'cls discipline Duration repetition id requirementId')
    InstanceSet = []
    for z, r in enumerate(RequirementSet):
        print(r)
        for i in range(r[3]):
            InstanceSet.append(Instance(cls=r[0], discipline=r[1], Duration=r[2], repetition=r[3], id=i, requirementId=z))
    for i in InstanceSet:
        print(i)

    # create decision varaibles
    model = cp_model.CpModel()
    Starts = {}
    Ends = {}
    Rooms = {}
    Teachers = {}
    for i in InstanceSet:
        start_var = model.NewIntVar(0, 77, name='start of instance %i' % (InstanceSet.index(i)))
        Starts[i] = start_var

        end_var = model.NewIntVar(0, 77, name='end of instance %i' % (InstanceSet.index(i)))
        Ends[i] = end_var

        room_var = model.NewIntVar(0, NbRoom, name='room of instance %i' % (InstanceSet.index(i)))
        Rooms[i] = room_var


        teacher_var = model.NewIntVar(0, NbTeacher, name='teacher of instance %i' % (InstanceSet.index(i)))
        Teachers[i] = teacher_var
    for i in InstanceSet:
        print(Starts[i], Rooms[i], Teachers[i])

    classTeacher = {}
    for c in Class:
        for  d in Discipline:
            c_d_var = model.NewIntVar(0, NbTeacher, name='class %i of d %i' % (Class.index(c), Discipline.index(d)))
            classTeacher[c, d] = c_d_var
            print(c_d_var)
    makespan = model.NewIntVar(0, 777, name = 'makespan')

    # now start writing constraints
    model.AddMaxEquality(makespan, Ends.values())
    durations = [0] * len(Class)
    for i in InstanceSet:
        # get sum of duration for each class
        durations[Class.index(i.cls)] =  durations[Class.index(i.cls)] + i.Duration
    print(durations)
    maxDUration = max(durations)
    print(maxDUration)
    model.Add(makespan >= maxDUration)

    #ensure the course ends after it starts
    for i in InstanceSet:
        model.Add(Ends[i] == Starts[i] + i.Duration)
    # // ensure course numerotation is chronological
    for i in InstanceSet:
        for j in InstanceSet:
            if i.id < j.id and i.requirementId == j.requirementId:
                model.Add(Starts[i] < Starts[j])

    #// ensure that a teacher is required once at any time point
    includesFLagsDict = {}
    for idx, r in enumerate(InstanceSet):
        for x in Teacher:
            if r.discipline in PossibleTeacherDiscipline[x]:

                includeFLags = []
                for idx1, o in enumerate(InstanceSet):
                    suffix = 't-r%i x%s o%i' % (idx, x, idx1)
                    includeFLags.append(model.NewIntVar(0, 1, suffix))

                includesFLagsDict[idx, x] = includeFLags
                for o in InstanceSet:
                    index = InstanceSet.index(o)
                    # O's displimne is not in the possible displine of teacher x, dont include this instance
                    if o.discipline not in PossibleTeacherDiscipline[x]:
                        model.Add(includeFLags[index] == False)
                        continue

                    startAfterR = model.NewBoolVar( 'start_% i after r' % (index))

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(startAfterR)
                    model.Add(Starts[o] < Starts[r]).OnlyEnforceIf(startAfterR.Not())

                    startBeforeEndR = model.NewBoolVar('start %i before end r' % (index))

                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(startBeforeEndR)
                    model.Add(Starts[o] >= Ends[r]).OnlyEnforceIf(startBeforeEndR.Not())

                    teacherX = model.NewBoolVar( 'same teacher %i for r' % (index))
                    model.Add(Teachers[o] == Teacher.index(x)).OnlyEnforceIf(teacherX)
                    model.Add(Teachers[o] != Teacher.index(x)).OnlyEnforceIf(teacherX.Not())

                    # if all three bools are true, include this o in the sum
                    model.Add(startAfterR + startBeforeEndR + teacherX == 3).OnlyEnforceIf(includeFLags[index])
                    model.Add(startAfterR + startBeforeEndR + teacherX < 3).OnlyEnforceIf(includeFLags[index].Not())
                model.Add(sum(includesFLagsDict[idx,x][InstanceSet.index(o)] for o in InstanceSet) < 2)
    # // ensure the teacher can teach the discipline
    for r in InstanceSet:
        v = []
        for i in PossibleTeacherIds[r.discipline]:
            v.append([i])

        #model.AddAllowedAssignments([Teachers[r]], v)
        if r.discipline == 'French':
            print(Teachers[r], r.discipline, v, Teacher[6])
            model.Add(Teachers[r] == 6)
        if r.discipline == 'Maths':
            model.Add(Teachers[r] == 0)
        if r.discipline == 'Economy':
            model.Add(Teachers[r] == 3)
        if r.discipline == 'Physics':
            model.Add(Teachers[r] == 5)
        if r.discipline == 'Geography':
            model.Add(Teachers[r] == 2)
    #  // ensure that a room is required once at any time point.
    includesFLagsRoomDict = {}
    for idx, r in enumerate(InstanceSet):

        for x in Room:
            if PossibleRoom[r.discipline, x] == 1:
                includesRoom = []
                for idx1, o in enumerate(InstanceSet):
                    suffix = 'r%i x%s o%i' % (idx, x, idx1)
                    includesRoom.append(model.NewIntVar(0, 1, suffix))
                includesFLagsRoomDict[idx, x] = includesRoom

                for o in InstanceSet:
                    index = InstanceSet.index(o)
                    if PossibleRoom[o.discipline, x] == 0:
                        model.Add(includesRoom[index] == False)
                        continue

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(includesRoom[index])
                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(includesRoom[index])

                    startAfterR = model.NewIntVar(0, 1, 'start_% i after %i' % (index, InstanceSet.index(r)))

                    model.Add(Starts[o] >= Starts[r]).OnlyEnforceIf(startAfterR)
                    model.Add(Starts[o] < Starts[r]).OnlyEnforceIf(startAfterR.Not())

                    startBeforeEndR = model.NewIntVar(0, 1, 'start %i before end %i' % (index, InstanceSet.index(r)))

                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(startBeforeEndR)
                    model.Add(Starts[o] >= Ends[r]).OnlyEnforceIf(startBeforeEndR.Not())

                    sameRoom = model.NewBoolVar('same room %i for r' % (index))
                    model.Add(Rooms[o] == Room.index(x)).OnlyEnforceIf(sameRoom)
                    model.Add(Rooms[o] != Room.index(x)).OnlyEnforceIf(sameRoom.Not())

                    # if all three bools are true, include this o in the sum
                    model.Add(startAfterR + startBeforeEndR + sameRoom  == 3).OnlyEnforceIf(includesRoom[index])

                    model.Add(startAfterR + startBeforeEndR + sameRoom < 3).OnlyEnforceIf(includesRoom[index].Not())

                model.Add(sum(includesFLagsRoomDict[idx, x][i] for i, v  in enumerate(InstanceSet)) < 2)

    for key, item in includesFLagsRoomDict.items():
        print(key, item)
    # // ensure the room can support the discipline
    for r in InstanceSet:
        print([r.discipline], [list(PossibleRoomIds[r.discipline])])
        model.Add(Rooms[r] <= 5)
        #model.AddAllowedAssignments([Rooms[r]],[(x,) for x in PossibleRoomIds[r.discipline]])
    #  // ensure that a class follows one course at a time
    sameCLassInstanceFLagsDict = {}
    for x in Class:
        for idx, r in enumerate(InstanceSet):

            if r.cls == x:
                sameCLassInstanceFLags = [model.NewBoolVar('')] * len(InstanceSet)

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
                    model.Add(Starts[o] <  Starts[r]).OnlyEnforceIf(classStartAfterR.Not())


                    model.Add(Starts[o] < Ends[r]).OnlyEnforceIf(classStartBeforeEndR)
                    model.Add(Starts[o] >= Ends[r]).OnlyEnforceIf(classStartBeforeEndR.Not())

                    model.Add(classStartAfterR + classStartBeforeEndR == 2).OnlyEnforceIf(sameCLassInstanceFLags[index])
                    model.Add(classStartAfterR + classStartBeforeEndR < 2).OnlyEnforceIf(sameCLassInstanceFLags[index].Not())

                sameCLassInstanceFLagsDict[x, idx] = sameCLassInstanceFLags
                #print(InstanceSet.index[o]for o in InstanceSet)
                model.Add(sum(sameCLassInstanceFLagsDict[x,idx][InstanceSet.index(o)] for o in InstanceSet) < 2)

    # // ensure that for given class and discipline, the teacher is always the same
    for x in Class:
        for d in Discipline:
            for r in InstanceSet:
                if r.cls == x and r.discipline == d:
                    model.Add(Teachers[r] == classTeacher[x, d])

    # breaks   // ensure a course starts and end the same halfday
    for idx, v in enumerate(InstanceSet):
        if v.Duration > 1:
            same_day_start = model.NewIntVar(0, MaxTime, 'same_day_s%i' % idx)
            same_day_end = model.NewIntVar(0, MaxTime, 'same_day_e%i' % idx)
            end_var_minus = model.NewIntVar(0, MaxTime, 'end_var_minus%i' % idx)
            model.Add(end_var_minus == Ends[v] - 1)
            model.AddDivisionEquality(same_day_start, Starts[v],  HalfDayDuration )
            model.AddDivisionEquality(same_day_end, end_var_minus, HalfDayDuration)
            model.Add(same_day_end ==  same_day_start)
    #  // insert break duration between specified disciplines
    for idx, i in enumerate(InstanceSet):
        for idx1, j in enumerate(InstanceSet):

            if idx1 <= idx: continue
            if i.cls == j.cls and (i.discipline, j.discipline) in NeedBreak or (j.discipline, i.discipline) in NeedBreak:
                print(i.discipline, j.discipline)
                # course do not belong to the same day
                differnt_day = model.NewBoolVar('%i-s-d-%i'% (idx, idx1))
                day_i = model.NewIntVar(0, MaxTime, 'day_%i' % (idx))
                day_j = model.NewIntVar(0, MaxTime, 'day_%i' % (idx1))
                model.AddDivisionEquality(day_i, Starts[i], DayDuration)
                model.AddDivisionEquality(day_j, Starts[j], DayDuration)
                model.Add(day_i != day_j).OnlyEnforceIf(differnt_day)

                # // courses do not belong to the same half day
                same_day = model.NewBoolVar('%i-d-d-%i'% (idx, idx1))
                half_day_i = model.NewIntVar(0, MaxTime, 's-day_%i' % (idx))
                half_day_j = model.NewIntVar(0, MaxTime, 's-day_%i' % (idx1))
                model.AddDivisionEquality(half_day_i, Starts[i], HalfDayDuration)
                model.AddDivisionEquality(half_day_j, Starts[j], HalfDayDuration)
                model.Add(half_day_i != half_day_j).OnlyEnforceIf(same_day)

                # / courses are separated by BreakDuration
                same_half_day = model.NewBoolVar('%i-h-d-%i'% (idx, idx1))
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
    #solver.parameters.max_time_in_seconds = 20
    print('solve', cp_model.INFEASIBLE)
    status = solver.Solve(model)
    print(status,  'Optimal = ', cp_model.OPTIMAL)
    if status == 4:
        roomsClasses = {}
        for x in Room:
            roomsClasses[x] = []
        for i in InstanceSet:
            print(i.discipline,  i.Duration, 'start = ', solver.Value(Starts[i]), 'end = ', solver.Value(Ends[i]))
        for i in InstanceSet:
            print(i.cls, i.discipline,  Room[solver.Value(Rooms[i])])
            room = Room[solver.Value(Rooms[i])]
            roomsClasses[room].append((solver.Value(Starts[i]), solver.Value(Ends[i])))
        print(solver.ObjectiveValue())
        for key, value in roomsClasses.items():
            print(key, value)
                #print(solver.Value(item))
create_model()