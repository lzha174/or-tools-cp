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
    print(Teacher)
    # get all displines
    Discipline = set()
    for t in TeacherDisciplineSet:
        Discipline.add(t[1])
    print(Discipline)
    Discipline = list(Discipline)

    HalfDayDuration = DayDuration /2
    MaxTime = DayDuration * NumberOfDaysPerPeriod
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
    NbRoom = len(Room)
    RoomId = range(NbRoom)
    PossibleRoomIds = {}
    for d in Discipline:
        s = set()
        for x in Room:
            if PossibleRoom[d,x] == True:
                s.add(Room.index(x))
        PossibleRoomIds[d] = s
    print(PossibleRoomIds)

    PossibleTeacherDiscipline = {}
    for x in Teacher:
        s = set()
        for d in Discipline:
            if (x, d) in TeacherDisciplineSet:
                s.add(d)
        PossibleTeacherDiscipline[x] = s
    NbTeacher = len(Teacher)
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
    print(PossibleTeacherIds)

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
        start_var = model.NewIntVar(0, MaxTime, name='start of instance %i' % (InstanceSet.index(i)))
        Starts[i] = start_var

        end_var = model.NewIntVar(0, MaxTime, name='end of instance %i' % (InstanceSet.index(i)))
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
    makespan = model.NewIntVar(0, MaxTime, name = 'makespan')
create_model()