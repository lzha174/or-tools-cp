from ortools.sat.python import cp_model

nbComputer = 'nbComputer'
allComputers = 'allComputers'
ComponentTypes = 'ComponentTypes'
Components = 'Components'
computer = 'computer'
def create_data():
    args = {}
    args['nbComputer'] = 5
    args['allComputers'] = range(0,6)
    args['ComponentTypes'] = ["CPU150", "CPU250", "CPU450", "Modem288", "Modem336", "Modem56K",
     "CDRW", "Cartridge", "Tape", "SoundCard", "VideoCard"]

    args['Components'] =  {'CPU150': (2,1,[]),
            'CPU250': (5,1,[]),
            'CPU450': (5,1,[]),
            'Modem288': (2,1,[]),
            'Modem336': (5,1,[]),
            'Modem56K': (5,1,[]),
            'CDRW': (5,1,['Tape']),
            'Cartridge': (5,1,[]),
            'Tape': (6,1,[]),
            'SoundCard': (2,2,[]),
            'VideoCard': (6,1,[])
    }
    args['computer'] =[['CPU450', 'Modem56K', 'CDRW', 'VideoCard', 'SoundCard'],
                ['CPU250', 'Modem336', 'Tape', 'VideoCard'],
                ['CPU250', 'Modem288', 'Cartridge', 'VideoCard', 'SoundCard'],
                ['CPU450', 'Modem56K', 'Tape', 'VideoCard', 'SoundCard'],
                ['CPU250', 'Modem336', 'CDRW', 'VideoCard'],
                ['CPU150', 'Modem288', 'Cartridge', 'VideoCard', 'SoundCard']];

    return args

def playAllowed():
    model = cp_model.CpModel()
    x = model.NewIntVar(0,10,name='x')
    y = model.NewIntVar(0,5,name='y')
    z = model.NewBoolVar(name='z')
    model.AddAllowedAssignments([x,y],[[4,1],[5,5]]).OnlyEnforceIf(z)
    model.Minimize(x+y)
    solver = cp_model.CpSolver()
    # solver.parameters.linearization_level = 0
    status = solver.Solve(model)
    print(status)
    if status == cp_model.MODEL_INVALID:
        print('invalid')
    if status == cp_model.FEASIBLE:
        print('feasible')
    if status == cp_model.INFEASIBLE:
        print('infeasible')
    if status == cp_model.OPTIMAL:
        print('ff')


def create_model():
    data = create_data()
    for a in data[allComputers]:
        for component in data[computer][a]:
            assert(component in data[ComponentTypes])

    UsedComponentTypes = []
    for a in data[allComputers]:
        for c in data[ComponentTypes]:
            if c in data[computer][a] and c not in UsedComponentTypes:
                UsedComponentTypes.append(c)


    for u in UsedComponentTypes:
        assert (u in data[ComponentTypes])

    HasIllegalFollowers = []
    for c in UsedComponentTypes:
        for d in UsedComponentTypes:
            if d in data[Components][c][2]:
                HasIllegalFollowers.append(c)
                break
    for h in HasIllegalFollowers:
        assert (h in data[ComponentTypes])

    componentInComputer = {}
    componentInComputer1 = {}
    for c in UsedComponentTypes:
        cInComputer = []
        cInComputer1 = []
        for a in data[allComputers]:
            if c in data[computer][a]:
                cInComputer.append(a)
                cInComputer1.append([a])
        componentInComputer[c] = cInComputer
        componentInComputer1[c] = cInComputer1
    print(componentInComputer1)
    for c in UsedComponentTypes:
        for u in componentInComputer[c]:
            assert (u >=0)
            assert (u <= data[nbComputer])

        model = cp_model.CpModel()
        orders = []
        # it means the ith order build computer orders[i]
        for a in data[allComputers]:
            orderVar  = model.NewIntVar(0, data[nbComputer], 'order_c_' + str(a))
            orders.append(orderVar)

        # constraint
        model.AddAllDifferent(orders)

        orderInComponent = {}
        for c in UsedComponentTypes:
            boolVars = []
            for a in data[allComputers]:
                orderInC = model.NewBoolVar(name='order_%i_in_%s' % (a, c))
                boolVars.append(orderInC)
                orderEquals = []
                #if (a==0 and c == 'CPU250'):
                #model.AddAllowedAssignments([orders[a]],componentInComputer1[c]).OnlyEnforceIf(orderInC)
                #model.AddForbiddenAssignments([orders[a]], componentInComputer1[c]).OnlyEnforceIf(orderInC.Not())
                for id in componentInComputer[c]:
                    orderEqualid = model.NewBoolVar(name='order_%i_equal_%i' % (a, id))
                    model.Add(orders[a] != id).OnlyEnforceIf(orderInC.Not())
                    model.Add(orders[a] ==id).OnlyEnforceIf(orderEqualid)
                    #model.Add(orders[a] != id).OnlyEnforceIf(orderEqualid.Not())
                    orderEquals.append(orderEqualid)
                model.AddBoolOr(orderEquals).OnlyEnforceIf(orderInC)
            orderInComponent[c] = boolVars

        for c in UsedComponentTypes:
            for p in range(0, data[nbComputer] + 1 - data[Components][c][0]):
                maxSeqBool  = model.NewBoolVar('max_seq_component_' + c+str(p))
                model.Add(sum(orderInComponent[c][s] for s in range(p, p+data[Components][c][0]-1 + 1)) == data[Components][c][0]).OnlyEnforceIf(maxSeqBool)
                # how to constraint a variable not in a set?
                # max sequence constraint
                model.Add(orderInComponent[c][ p+data[Components][c][0]] == False).OnlyEnforceIf(maxSeqBool)
                model.Add(sum(orderInComponent[c][s] for s in range(p, p + data[Components][c][0] - 1 + 1)) <
                          data[Components][c][0]).OnlyEnforceIf(maxSeqBool.Not())
            model.Add(sum(orderInComponent[c][s] for s in range(0, data[Components][c][1])) >=
                          data[Components][c][1]).OnlyEnforceIf(orderInComponent[c][0])
            for p in range(0, data[nbComputer] - data[Components][c][1] + 1):
                orderInPPlusOne = model.NewBoolVar(name='order_%i_in_plus_%s' % ( p,c))
                #model.Add(orderInComponent[c][p] == False).OnlyEnforceIf(orderInPPlusOne)
                #model.Add(orderInComponent[c][p+1] == True).OnlyEnforceIf(orderInPPlusOne)
                model.AddImplication(orderInPPlusOne, orderInComponent[c][p].Not())
                model.AddImplication(orderInPPlusOne, orderInComponent[c][p+1])
                FFBool  = model.NewBoolVar(name='order_%i_in_plus_%s' % ( p,c))
                TTBool = model.NewBoolVar(name='order_%i_in_plus_%s' % ( p,c))
                TFBool = model.NewBoolVar(name='order_%i_in_plus_%s' % ( p,c))
                model.AddImplication(FFBool, orderInComponent[c][p].Not())
                model.AddImplication(FFBool, orderInComponent[c][p+1].Not())

                model.AddImplication(TTBool, orderInComponent[c][p])
                model.AddImplication(TTBool, orderInComponent[c][p+1])

                model.AddImplication(TFBool, orderInComponent[c][p])
                model.AddImplication(TFBool, orderInComponent[c][p+1].Not())
                # at least one of these is true
                model.AddBoolOr([FFBool,TTBool,TFBool]).OnlyEnforceIf(orderInPPlusOne.Not())

                #model.AddAllowedAssignments([orderInComponent[c][p], orderInComponent[c][p + 1]],
                #                            [[False, True]]).OnlyEnforceIf(orderInPPlusOne)
                # add min seq constriant
                model.Add(sum(orderInComponent[c][s] for s in range(p+1, p+data[Components][c][1]+1)) >= data[Components][c][1]).OnlyEnforceIf(orderInPPlusOne)
                #model.AddAllowedAssignments([orderInComponent[c][p], orderInComponent[c][p+1]],[[False, False], [True, True], [True,False]]).OnlyEnforceIf(orderInPPlusOne.Not())
                # orderInPPlusOne = False, what does this mean?
        for c in HasIllegalFollowers:
            for a in range(0,5):
                for c2 in UsedComponentTypes:
                    if c2 in data[Components][c][2]:
                        # c2 is a illegal follwer
                        model.Add(orderInComponent[c2][a+1] == False).OnlyEnforceIf(orderInComponent[c][a])
    # this is the ilog solution
    #model.AddAllowedAssignments(orders,[[1,2,0,4,5,3]])
    solver = cp_model.CpSolver()
    # solver.parameters.linearization_level = 0
    status = solver.Solve(model)
    print(status)
    if status == cp_model.MODEL_INVALID:
        print('invalid')
    if status == cp_model.FEASIBLE:
        print('feasible')
    if status == cp_model.INFEASIBLE:
        print('infeasible')
    if status == cp_model.OPTIMAL:
        print('heyhey')
        for a in data[allComputers]:
            print(solver.Value(orders[a]))


playAllowed()
