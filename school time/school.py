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

    HalfDayDuration = DayDuration /2
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

                #includesRoom = [model.NewIntVar(0 ,1, 'room for %i at r %s ins %i ' ]
                includesFLagsRoomDict[idx, x] = includesRoom

                #model.Add(includesFLagsRoomDict[idx,x][0] == 1)
                #model.Add(includesFLagsRoomDict[idx,x][1] == 0)

                #model.Add(includesRoom[0] + includesRoom[1] == 1)
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

    # // ensure that for given class and discipline, the teacher is always the same
    for x in Class:
        for d in Discipline:
            for r in InstanceSet:
                if r.cls == x and r.discipline == d:
                    a = 1
                    model.Add(Teachers[r] == classTeacher[x, d])
    model.Minimize(makespan)
    solver = cp_model.CpSolver()
    #solver.parameters.max_time_in_seconds = 20
    print('solve', cp_model.INFEASIBLE)
    status = solver.Solve(model)
    print(status,  'Optimal = ', cp_model.OPTIMAL)
    if status == 4:
        for i in InstanceSet:
            print(i.discipline,  i.Duration, 'start = ', solver.Value(Starts[i]), 'end = ', solver.Value(Ends[i]))
        for i in InstanceSet:
            print(i.cls, i.discipline,  Room[solver.Value(Rooms[i])])
        print(solver.ObjectiveValue())
        for key in includesFLagsRoomDict:
            print(includesFLagsRoomDict[key])
            for item in includesFLagsRoomDict[key]:
                print('item=',solver.Value(item))
                #print(solver.Value(item))
create_model()