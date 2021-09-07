# or-tools-cp
in stateCP subfolder, we implement alwaysEqual for a state variable over intervals. Refer to Cplex constraint programming for details of the meaning.
Output will be like:

job id 0 0 10

job id 1 11 15

job id 2 16 30

state =  1 [[0, 10]]

state =  2 [[11, 15], [16, 30]]

This shows the start, and finish time for each job, and the state value over each the whole period
 
