import collections

# Import Python wrapper for or-tools CP-SAT solver.
from ortools.sat.python import cp_model

from CPextension import cumulative

def create_model():
    model = cumulative.ModelComulative()

    SizeSquare = 112
    NbSquares = 21
    model.setMaxValue(SizeSquare)
    Squares = range(NbSquares)
    Size = [50, 42, 37, 35, 33, 29, 27, 25, 24, 19, 18, 17, 16, 15, 11, 9, 8, 7, 6, 4, 2]

    task_type = collections.namedtuple('task', 'start end duration interval')
    X = {}
    Y = {}
    x_intevals = []
    y_intervals = []
    for i in Squares:
        name = 'x_%i'.format(i)
        start_v = model.NewIntVar(0, SizeSquare, 'start' + name)
        duration = Size[i]
        end_v = model.NewIntVar(0, SizeSquare, 'end' + name)
        interval = model.NewIntervalVar(start_v, duration, end_v, 'interval' + name)
        X[i] = task_type(start = start_v, end = end_v, duration = duration, interval = interval)
        x_intevals.append(X[i])

    for i in Squares:
        name = 'y_%i'.format(i)
        start_v = model.NewIntVar(0, SizeSquare, 'start' + name)
        duration = Size[i]
        end_v = model.NewIntVar(0, SizeSquare, 'end' + name)
        interval = model.NewIntervalVar(start_v, duration, end_v, 'interval' + name)
        Y[i] = task_type(start = start_v, end = end_v, duration = duration, interval = interval)
        y_intervals.append(Y[i])
    # cumulFunction rx = sum(s in Squares) pulse(x[s], Size[s]);



    model.AddComulativeCustom(x_intevals, Size, SizeSquare, 'x_cumulative')
    model.AddComulativeCustom(y_intervals, Size, SizeSquare, 'y_cumulative')

    model.alwaysIN(0, SizeSquare, SizeSquare, SizeSquare, 'x_cumulative')
    model.alwaysIN(0, SizeSquare, SizeSquare, SizeSquare, 'y_cumulative')
    for i in Squares:
        for j in Squares:
            if j<=i: continue
            # x_i end before x_j start
            x_i_end_b4_x_j = model.NewBoolVar('x_%i_end_b4_x_%i'.format(i,j))
            # x_j end before x_i start
            x_j_end_b4_x_i = model.NewBoolVar('x_%i_end_b4_x_%i'.format(j, i))
            # y_i end before y_j start
            y_i_end_b4_y_j = model.NewBoolVar('y_%i_end_b4_y_%i'.format(i, j))
            # y_j end before y_i start
            y_j_end_b4_y_i = model.NewBoolVar('y_%i_end_b4_y_%i'.format(j, i))
            model.Add(X[i].end <= X[j].start).OnlyEnforceIf(x_i_end_b4_x_j)
            model.Add(X[j].end <= X[i].start).OnlyEnforceIf(x_j_end_b4_x_i)
            model.Add(Y[i].end <= Y[j].start).OnlyEnforceIf(y_i_end_b4_y_j)
            model.Add(Y[j].end <= Y[i].start).OnlyEnforceIf(y_j_end_b4_y_i)
            model.AddBoolXOr([x_i_end_b4_x_j, x_j_end_b4_x_i, y_i_end_b4_y_j, y_j_end_b4_y_i])

    solver = cp_model.CpSolver()

    solver.parameters.enumerate_all_solutions = False
    status = solver.Solve(model)
    print(status,  'Optimal = ', cp_model.OPTIMAL)
    print('X')
    for key, item in X.items():
        print(solver.Value(item.start), solver.Value(item.end))
    print('Y')
    for key, item in Y.items():
        print(solver.Value(item.start), solver.Value(item.end))
    model.show_cum(solver)

create_model()

