# Import Python wrapper for or-tools CP-SAT solver.
# implement difference between two cumulatives
#implemnt alwaysin for the cumulative function
from ortools.sat.python import cp_model

class ModelComulative(cp_model.CpModel):
    def __init__(self):
        cp_model.CpModel.__init__(self)
        self.comulatives = {}
        self.max_capacity = 10
    def setMaxValue(self, value):
        self.max_capacity = value

    def AddComulativeCustom(self, intervals, demands, horizon, name, consdier_present = False ):
        comulative_vars = [self.NewIntVar(0, self.max_capacity, 'cum '+ name +' '+ str(t)) for t in range(0, horizon )]
        # bacilly cumulative constraint is very simple
        # create a intvar for every discrete timestamp in the horizon, at every stamp, if an interval contains this timestamp
        # add the demand for that interval, the sum will be equal to the intvar at that time stamp
        # when we constrain capacity, go through each timestap and add a constraint for capacity
        # if we have multiple cumulative vars, we can even do add or subtract between these vars for each time stamp
        #print(comulative_vars)
        for t in range(0, horizon):
            totalDemand = 0
            in_range_vars = [self.NewIntVar(0, 1, 'in '+ name +' '+ str(i) + str(t)) for i in range(len(intervals))]
            for idx, interval in enumerate(intervals):
                # only consider this interval if it is present
                # if present = false, consdier = false
                # if consdier the false, skip this no constraint at all
                present = True
                if consdier_present:
                    present = interval.present
                # if interval.start <= t < interval.end total = +demand[interval]
                in_range = self.NewBoolVar(name + str(idx) + 'in' + str(t))
                # I like the OnlyEnfoceIf can use a list of literals, all of them must be true.
                # I can pass True/Fasle into the list
                self.Add(in_range_vars[idx] == 1).OnlyEnforceIf([in_range, present])
                self.Add(t - interval.start >= 0).OnlyEnforceIf([in_range, present])
                self.Add(interval.end - t > 0).OnlyEnforceIf([in_range, present])

                before_t = self.NewBoolVar(str(idx) + 'b4' + name +' '+ str(t))
                self.Add(interval.end <= t).OnlyEnforceIf([before_t,present])
                self.Add(in_range_vars[idx] == 0).OnlyEnforceIf([before_t, present])

                after_t = self.NewBoolVar(str(idx) + 'after' + name + ' ' + str(t))
                self.Add(interval.start > t).OnlyEnforceIf([after_t, present])
                self.Add(in_range_vars[idx] == 0).OnlyEnforceIf([after_t, present])

                self.AddBoolOr([in_range, before_t, after_t])

            self.Add(cp_model.LinearExpr.ScalProd(in_range_vars, demands) == comulative_vars[t])

        self.comulatives[name] = comulative_vars
    def difference(self, name1, name2, min_v, max_v):
        horizon = len(self.comulatives[name2])
        for t in range(horizon):
            print(self.comulatives[name1][t],self.comulatives[name2][t]  )
            self.Add(self.comulatives[name1][t] - self.comulatives[name2][t] >= min_v)
            self.Add(self.comulatives[name1][t] - self.comulatives[name2][t]  <= max_v)

    def alwaysIN(self, start, end, vmin, vmax, name):
        for t in range(start, end):
            self.Add(self.comulatives[name][t] >= vmin)
            self.Add(self.comulatives[name][t] <= vmax)


    def show_cum(self, solver):
        for key in self.comulatives:
            print('key=',key)
            for item in self.comulatives[key]:
                print('cum value', solver.Value(item))