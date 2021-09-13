# this script models a talent scheduling problem from Cplex Stuodio Tutorial, refer the attached pdf
# author: oddo z
from ortools.sat.python import cp_model

def createModel():
    numActors = 5
    actors = range(numActors)
    numScenes = 9
    scenes = range(numScenes)
    sceneDuration = [2,4,1,3,3,2,5,7,6]
    actorPay = [1,1,1,1,1]
    actorInScene =[[1, 1, 0, 1, 0, 1, 1, 0, 1],
     [1, 1, 0, 1, 1, 1, 0, 1, 0],
     [1, 1, 0, 0, 0, 0, 1, 1, 0],
     [1, 0, 0, 0, 1, 1, 0, 0, 1],
     [0, 0, 1, 0, 1, 1, 1, 1, 0]];
    model = cp_model.CpModel()
    sceneVars = []
    slotVars = []
    for scene in scenes:
        suffix = 'slot of scene' + str(scene)
        # define the slot for each scene
        sceneVar = model.NewIntVar(0,numScenes-1,name=suffix)
        sceneVars.append(sceneVar)
        suffix = 'scene id for slot  '+str(scene)
        slotVar = model.NewIntVar(0, numScenes-1, name= suffix)
        slotVars.append(slotVar)
    firstSlots = []
    lastSlots = []
    waitTimes = []
    for actor in actors:
        suffix = 'first slot of actor '+str(actor)
        firstVar = model.NewIntVar(0, numScenes-1, name=suffix)
        firstSlots.append(firstVar)
        suffix= 'last slot of actor' + str(actor)
        lastVar = model.NewIntVar(0, numScenes-1,name=suffix)
        lastSlots.append(lastVar)
        suffix = 'wait time of actor' + str(actor)
        waitVar = model.NewIntVar(0,1000,name=suffix)
        waitTimes.append(waitVar)

    # first add inverse constraint
    model.AddInverse(sceneVars,slotVars)

    # define the first slot var and lost slot var
    for actor in actors:
        scenesIn = []
        for scene in scenes:
            if actorInScene[actor][scene] == 1:
                scenesIn.append(slotVars[scene])
        # this is the slot number for each scene, not the scene number in each slot
        model.AddMinEquality(firstSlots[actor], scenesIn)
        model.AddMaxEquality(lastSlots[actor], scenesIn)
    # define the wait time
    waitExper = {}
    for actor in actors:
        waitExper[actor] = []
        for scene in scenes:
            if actorInScene[actor][scene] == 1: continue
            # this is how to use bools to define constraints under different variable values
            # must consider all possible scenarios, only one scenario will happen. So every scenario has a bool
            # the sum of the bools is 1
            waitSlot = model.NewBoolVar(name='between first and second for actor_%i at slot %i' % (actor, scene))
            waitExper[actor].append ((waitSlot, sceneDuration[scene]))
            # if waitSlot is True, the slot of the scene is between the frist and last
            model.Add(slotVars[scene] >= firstSlots[actor]).OnlyEnforceIf(waitSlot)
            model.Add(slotVars[scene] <= lastSlots[actor]).OnlyEnforceIf(waitSlot)
            # if waitSlot is False, the slot of this scene is outside, either before first or after last, need two new bools to consider each scenario
            afterLast = model.NewBoolVar(name='after last for actor_%i' % (actor))
            beforeFirst = model.NewBoolVar(name='before first for actor _%i' % (actor))
            # only one of these bools will be true
            model.Add(waitSlot +  afterLast + beforeFirst== 1)

            model.Add(slotVars[scene] <= firstSlots[actor] -1).OnlyEnforceIf(beforeFirst)
            model.Add(slotVars[scene] >= lastSlots[actor] +1).OnlyEnforceIf(afterLast)
    # get total wait time for each actor
    for actor in actors:
        model.Add(sum(wait[0]*wait[1] for wait in waitExper[actor]) == waitTimes[actor])
    # get objective
    obj = model.NewIntVar(0,100000,name='obj')
    model.Add(sum(actorPay[actor] * waitTimes[actor] for actor in actors)==obj)
    model.Minimize(obj)
    solver = cp_model.CpSolver()
    solver.parameters.linearization_level = 0
    status = solver.Solve(model)
    print(status)
    if status == cp_model.OPTIMAL:
        print('total cost is', solver.Value(obj))
        print('slot for each scene:')
        for scene in scenes:
            s = ("scene {} is at slot {}".format(scene, solver.Value(sceneVars[scene])))
            print(s)
        for scene in scenes:
            s = ("slot {} is doing scene {}".format(scene, solver.Value(slotVars[scene])))
            print(s)

createModel()