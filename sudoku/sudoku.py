from ortools.sat.python import cp_model


# do a 9x9 matrix sudoku solver
# writer: oddo zhang
def main():
    model = cp_model.CpModel()
    length = range(0, 9)
    vars = {}
    rows = {}
    columns = {}
    # each 3x3 square must have differnt numbers
    subsquares = {}

    for i in range(0, 3):
        for j in range(0, 3):
            subsquares[(i, j)] = []

    for i in length:
        rows[i] = []
        for j in length:
            if j not in columns:
                columns[j] = []
            var = model.NewIntVar(lb=1, ub=9, name='i_%i_j_%i' % (i, j))
            rows[i].append(var)
            columns[j].append(var)
            # find out which subsqure this cell is in
            subrow = int(i / 3)
            subcol = int(j / 3)
            subsquares[(subrow, subcol)].append(var)
            vars[(i, j)] = var
    # initialise know cells

    values = [(0, 0, 5), (0, 5, 7), (0, 8, 3),
              (1, 3, 9), (1, 4, 1),
              (2, 0, 1), (2, 5, 3), (2, 6, 2), (2, 8, 4),
              (3, 1, 8), (3, 3, 1), (3, 7, 7),
              (4, 7, 6),
              (5, 0, 2), (5, 5, 4),
              (6, 2, 6), (6, 3, 8),
              (7, 1, 5), (7, 8, 6),
              (8, 1, 7), (8, 2, 3), (8, 3, 2), (8, 7, 9)]
    for v in values:
        model.Add(vars[v[0], v[1]] == v[2])

    for row in length:
        # for each row the numbers are differnt
        model.AddAllDifferent(rows[row])

    for column in length:
        # for each column numbers are differnt
        model.AddAllDifferent(columns[column])
    # for each sub 3x3 matrix, numbers are different
    for key, items in subsquares.items():
        model.AddAllDifferent(items)
    solver = cp_model.CpSolver()
    # solver.parameters.linearization_level = 0
    status = solver.Solve(model)
    if status == cp_model.OPTIMAL:
        print('heyhey')
        output = ''
        for i in length:

            for j in length:
                output += str(solver.Value(vars[i, j])) + ' '
            output += '\n'
        print(output)


main()
