# try apply online optimisation
# when a new task arrives in the system, for all tasks that have not started, we can reschedule them
# if these tasks have not started, their followed up tasks will not be processed yet, once we rechedule them, we need to
# remove the follow up tasks from the queue and reinsert that with new ready time?
import pandas as pd

from initial_paras import *
from operator import itemgetter
import random
# in a more real world, jobs have priority, what does this mean?
# this means if job a, b are both ready, a has priority 1, b has priority 2, we should take a
# after finish first task  of a, second task is ready, where should I insert? sill insert by ready time as this is when we can start processing the second task
# in real word, but if there is another task c already ready before a in the queue for the same station,  even task of job c is already there
# we should still put a before c to process, in this sence, c will be deplayed. By jump the queue, even a task is ready, it will be served later,
# but this does not mean its finish time is delayed. If there are mutliple servers, we schuedule a server to do task a, later, we can still schedule
# task c to a server that has been free for a long time, it will still process task c at its ready time
# if there is only one server, then task c will be definitely delayed, as the server must process a first, then c

# for each worker I want to know all the idle intervals so far
class WorkerClass:
    def __init__(self, idx, day, period, start_time, end_time, lunch_start_time, lunch_end_time, stage):
        self.idx = idx
        self.day = day
        self.period = period
        self.start_time = start_time
        self.end_time = end_time
        self.last_task_finish_time = self.start_time
        self.total_busy_time = 0
        self.tasks_done_count_before_break = 0
        self.first_task_start_before_break = None
        # I want to add some break constraint
        # first lunch break between 1pm -2pm
        self.lunch_start_time = lunch_start_time
        self.lunch_end_time = lunch_end_time
        self.lunch_interval = pd.Interval(self.lunch_start_time, self.lunch_end_time, closed = 'left')
        self.total_breaks = 0
        self.task_gantt = []
        self.idle_intervals = []
        self.total_tasks_assgined = 0
        self.interval_modify = None
        self.stage =  stage
        # I want to add a maximum nb of tasks you can do for each wokrer per hour
        if stage != paras[batch_stage_idx_str] and stage != nigh_stage:

            # insert the lunch as a fake task, this may create idle interval before  lunch, marke the last finish task as lunch finish task
            if lunch_start_time >= start_time and lunch_end_time <= end_time:
                self.idle_intervals.append(pd.Interval(left = start_time, right = lunch_start_time, closed = 'left'))
                self.last_task_finish_time = lunch_end_time

    def reschedule(self, new_task, tasks_reschedule):
        # remove old tasks from gant chart
        idx_to_remove = []
        new_gantt = []
        all_new_tasks = tasks_reschedule[0]
        #for old_task in tasks_reschedule[0]:
        #    print('task to schedule', old_task)
        avaliabletime = tasks_reschedule[1]
        for idx, scheduled in enumerate(self.task_gantt):
            include_task = True
            for old_task in tasks_reschedule[0]:
                if scheduled[6] == old_task:
                    include_task = False
                    break
            if include_task:
                new_gantt.append(scheduled)
        self.task_gantt = new_gantt
        task_start_time = avaliabletime
        #print('start schedule at', format_time(task_start_time))
        nb_tasks_assigned = 0
        # sort all to-be-scheduled tasks in duration

        for t in all_new_tasks:
            #print('new task', t)

            if task_start_time + t.duration > self.end_time:
                 break
            else:
                task_start_time = max(task_start_time, t.ready_time)
                if task_start_time in self.lunch_interval:
                    task_start_time = self.lunch_end_time
                task_start_time = task_start_time + t.duration
                nb_tasks_assigned = nb_tasks_assigned + 1
        #print(format_time(task_start_time), nb_tasks_assigned)

        return task_start_time, all_new_tasks

    def fix_reschedule(self, allJobs, all_new_tasks, final_avaliable_time):
        unscheduled = []
        rescheduled = []

        for rescheduled_task in all_new_tasks:
            #print('rescheduled', rescheduled_task)

            task_start_time = max(final_avaliable_time, rescheduled_task.ready_time)

            if task_start_time + rescheduled_task.duration in self.lunch_interval:
                task_start_time = self.lunch_end_time

            if task_start_time + rescheduled_task.duration > self.end_time:
                rescheduled_task.task_finished = False
                if rescheduled_task.job_id == 2145 and rescheduled_task.client_idx == 1:
                    print('f')
                unscheduled.append(rescheduled_task)
            else:
                final_avaliable_time = task_start_time + rescheduled_task.duration
                if rescheduled_task.task_finished == False:
                # this is a new task
                    self.update_gantt(rescheduled_task, rescheduled_task.duration, task_start_time)
                    allJobs.mark_job_task_finish(rescheduled_task.job_id, task_start_time)

                else:
                    allJobs.update_previous_task(rescheduled_task.job_id, task_start_time)
                    self.update_gantt(rescheduled_task, rescheduled_task.duration, task_start_time)
                    #print('updated rescheduled', rescheduled_task)
                rescheduled.append(rescheduled_task)

        return unscheduled, rescheduled


    def find_not_start_tasks(self, task_ready_time, allJobs):
        changble_tasks = []
        last_scheduled_finish_time = self.start_time
        for scheduled_task in self.task_gantt:
            #print('task in gantt', scheduled_task[6])
            scheduled_start_time = scheduled_task[4]
            if scheduled_start_time >= task_ready_time:
                changble_tasks.append(scheduled_task)
            else:
                t = scheduled_task[6]
                if allJobs[t.job_id].current_task_idx == len(allJobs[t.job_id].taskcollection):
                    allJobs[t.job_id].job_done = True
                if scheduled_start_time + scheduled_task[5] > last_scheduled_finish_time:
                    last_scheduled_finish_time =  scheduled_start_time + scheduled_task[5]
        return  changble_tasks, last_scheduled_finish_time

    def find_insertion(self, task_ready_time, duration):
        # return task start time and if this task is appended to the end
        # go through the array of idle itnervals, find the first idle itnerval that can allocate this task
        index_to_modify = None
        left_interval = None
        right_interval = None
        task_start_time = None
        for idx, interval in enumerate(self.idle_intervals):
            left = interval.left
            right = interval.right
            #print(format_time(left), format_time(right))
            assert(self.lunch_interval.overlaps(interval) == False)
            # if right is below task ready time, no way to allocate this task to this interval
            # so the first interval must be that task ready time in the interval or left is above task ready time
            if right < task_ready_time: continue
            if left >= task_ready_time or task_ready_time in interval:
                task_start_time = custom_max(left, task_ready_time)
                task_end_time = task_start_time + duration
                if task_end_time in interval:
                    # this is a valid intrval
                    # once this task is assgined, two more idle intervals can be created
                    if task_start_time > left:
                        left_interval = pd.Interval(left = left, right = task_start_time, closed = 'left')
                    if task_end_time < right:
                        right_interval = pd.Interval(left = task_end_time, right = right, closed = 'left')
                    index_to_modify = idx
                    break
        if index_to_modify is not None:
            self.interval_modify = modify_worker_interval_type(index = index_to_modify, left_interval = left_interval, right_interval = right_interval)
            #self.update_gantt(duration, task_start_time)
            return task_start_time, False
        else:
            self.interval_modify = None
            # no place to insert between idle intervals
            # return the position after next avalible time
            task_start_time = custom_max(self.last_task_finish_time, task_ready_time)
            if task_start_time + duration > self.end_time:
                return None, False
            #print(format_time(self.last_task_finish_time), format_time(task_ready_time))
            return custom_max(self.last_task_finish_time, task_ready_time), True

    def modify_idle_interval(self):
        #self.show_idle_intervals()
        del self.idle_intervals[self.interval_modify.index]
        insert_idx = self.interval_modify.index
        if self.interval_modify.left_interval is not None:
            self.idle_intervals.insert(insert_idx, self.interval_modify.left_interval)
            insert_idx = insert_idx + 1
        if self.interval_modify.right_interval is not None:
            self.idle_intervals.insert(insert_idx, self.interval_modify.right_interval)

    def show_idle_intervals(self):
        for interval in self.idle_intervals:
            print(format_time(interval.left), format_time(interval.right))

    def update_last_task_finish_time(self, task_start_time, duration):
        # total busy time is used to calculate utilisation
        previous_avaliable_time = self.last_task_finish_time

        # notebook change to python_max
        # if next avlaible time is 12:50, how to make it pass lunch time??

        # how to make avalible time jump out of ounch time
        self.last_task_finish_time = task_start_time + duration
        #print('last finish time', format_time(self.last_task_finish_time))

        if self.stage != paras[batch_stage_idx_str] and self.stage != nigh_stage:
            # No task assigned yet, no idle interval
            if len(self.idle_intervals) == 0:
                self.total_tasks_assgined = self.total_tasks_assgined + 1
                # have an idle interval between previous avaliable time and task start time
            assert (task_start_time >= previous_avaliable_time)
            if (task_start_time > previous_avaliable_time):
                self.idle_intervals.append(pd.Interval(left = previous_avaliable_time, right = task_start_time, closed='left'))


    def update_gantt(self, task, duration, task_start_time):
        task_gantt = [self.day, self.period, self.stage, self.idx, task_start_time, duration, task]
        self.task_gantt.append(task_gantt)

    def get_avaliable_time(self):
        return self.last_task_finish_time

    def __str__(self):
        start = format_time(self.start_time)
        end = format_time(self.end_time)
        avaliable = format_time(self.last_task_finish_time)
        f = f'woker start {start} end {end} avaliabe {avaliable}'
        return f

    def get_utilisation(self):
        return self.total_busy_time / (self.end_time - self.start_time)

    def isLunchTime(self, task_start_time, task_finish_time):
        if self.stage == paras[batch_stage_idx_str]: return
        task_inveral = pd.Interval(task_start_time, task_finish_time)
        isLunch = self.lunch_interval.overlaps(task_inveral)
        if isLunch:
            # make the avaliable time pass lunch
            self.last_task_finish_time = self.lunch_end_time


class WokrerCollection:
    def __init__(self, day, period, stage, nb_worker, start_time, end_time, lunch_start_time, lunch_end_time):
        self.workers = [WorkerClass(i, day, period, start_time, end_time, lunch_start_time, lunch_end_time, stage) for i
                        in range(nb_worker)]
        self.stage = stage
        self.start = format_time(start_time)
        self.end = format_time(end_time)
        self.day = day
        self.period = period

    def __str__(self):
        output = [f'stage {self.stage} with {len(self.workers)} workers start {self.start} end {self.end}']
        for w in self.workers:
            output.append(w.__str__())
        if self.stage != paras[batch_stage_idx_str] and self.stage != nigh_stage:
            for w in self.workers:
                output.append(w.__str__())

        return ('\n').join(o for o in output)

    def get_break_stats(self):
        output = []
        for w in self.workers:
            if self.stage != paras[batch_stage_idx_str] and self.stage != nigh_stage:
                output.append(w.total_breaks)
        return output

    def get_gantt(self):
        output = []
        for w in self.workers:
            output = output + w.task_gantt
        return output

    def average_utilisation(self):
        average = 0
        for worker in self.workers:
            average = average + worker.get_utilisation()
        average = average / len(self.workers)
        return average

    def show_idle_intervals(self):
        for worker in self.workers:
            for interval in worker.idle_intervals:
                left = format_time(interval.left)
                right = format_time(interval.right)
                print(left, right)
    def reschedule(self, task, allJobs):
        worker_reschedule = {}
        for worker in self.workers:

            # find all tasks that have not started?
            #print(worker)
            if task.client_idx != paras[batch_stage_idx_str]:

                current_time = max(worker.start_time, task.ready_time)
                #print(worker, 'current event time', format_time(current_time))

                changable_tasks, last_scheduled_finish_time = worker.find_not_start_tasks(task.ready_time, allJobs)
                #print('last scheduled finish time ', format_time(last_scheduled_finish_time))
                sorted_tasks = sorted(changable_tasks, key=itemgetter(4))
                sorted_tasks = [s[6] for s in sorted_tasks] + [task]
                sorted_tasks = sorted(sorted_tasks, key = lambda x: x.duration)
                worker_reschedule[worker] = (sorted_tasks, last_scheduled_finish_time)
                #for t in sorted_tasks:
                #    print('changable, ', t)
        return worker_reschedule

    def insert_into_idle(self, task, duration):
        # find the worker with earliest insertion
        # it could be the end of the worker current task list if we cant insert into idle intervals between already allocated tasks
        min_start_time = float('inf')
        best_worker = None
        isAppend = True
        for worker in self.workers:
            # find all tasks that have not started?
            #print(worker)
            task_start_time, append = worker.find_insertion(task.ready_time, duration)

            if task_start_time is not None:
                if task_start_time < min_start_time:
                    min_start_time = task_start_time
                    best_worker = worker
                    isAppend = append

        if best_worker is not None:
            if isAppend:
                best_worker.update_last_task_finish_time(task_start_time=min_start_time, duration = duration)
            else:
                best_worker.modify_idle_interval()
            best_worker.total_busy_time = best_worker.total_busy_time + duration
            best_worker.update_gantt(task, duration, min_start_time)
        return best_worker, min_start_time

    def next_avaliable_worker(self, ready_time, duration):
        next_avalible_time = None
        next_worker = None
        for worker in self.workers:
            # if worker.get_avaliable_time() < ready_time: continue
            # if task ready time is during lunch time, avalible time is 12pm, task start time is task ready time
            # it overlap with lunch time, so make the avalible time for that worker to be after lunch time for this task
            # if task ready time is before lunch, avalible time is during lunch time, task start time is during lunch time, again overlap, make avalible time after lunch
            # notebook change to python_max
            task_start_time = custom_max(ready_time, worker.last_task_finish_time)
            task_finished_time = task_start_time + duration
            # if this work overlap with lunch time, change the avaliable time for the worker to b after lunch time
            worker.isLunchTime(task_start_time, task_finished_time)
            # reset start time
            # task_start_time = custom_max(ready_time, worker.next_avalaible_time)
            # task_finished_time = task_start_time + duration

            if task_finished_time > worker.end_time: continue
            # if this job overlap lunch time, pretend this worker is avalible after lunch

            # worker.isLunchTime(task_start_time, task_finished_time)

            if next_avalible_time is None:
                next_avalible_time = worker.last_task_finish_time
                next_worker = worker
            else:
                if worker.last_task_finish_time < next_avalible_time:
                    next_avalible_time = worker.last_task_finish_time
                    next_worker = worker
        return next_worker


class TaskClass:

    def __init__(self, task, job_id=0):
        self.task = task
        self.start_task_time = 0
        self.duration = task.duration
        self.end_task_time = 0
        self.task_finished = False
        self.task_finish_time = 0
        self.ready_time = task.ready_time
        self.client_idx = task.client_idx
        self.priority = task.priority_idx
        self.first_task_ready_time = task.first_task_ready_time
        self.job_id = job_id
        self.task_rank = task.task_rank
        self.finished_data = None



    def __str__(self):
        stage = self.task.client_idx
        client = paras[idx_to_name_client_str][stage]
        return (
            f'job {self.job_id} task {client} start {format_time(self.start_task_time)} end {format_time(self.end_task_time)} rank ={self.task_rank} duration {self.duration} ready {format_time(self.ready_time)}')

    def __lt__(self, other):
        # for the same stage, queue high rank job to schedule first, rank 0 is highest
        return self.ready_time < other.ready_time

    def get_task(self):
        return self.task

    def get_first_task_ready_time(self):
        return self.first_task_ready_time

    def set_finish(self, time):
        self.task_finished = True
        self.task_finish_time = time

    def set_start_time(self, task_start_time=0):
        self.start_task_time = task_start_time
        self.end_task_time = task_start_time + self.duration
        self.task_finished = True

        start_time = format_time(self.start_task_time)
        end_time = format_time(self.end_task_time)
        ready_time = format_time(self.ready_time)

        task_name = paras["idx_to_name_client"][self.task.client_idx]

        case_key_idx = self.task.case_key_idx
        case_name = paras[idx_to_name_key_str][case_key_idx]
        priority = paras[idx_to_name_priority_str][self.priority]
        self.finished_data = [case_key_idx, case_name, priority, task_name, self.task.order, start_time, end_time,
                         self.duration, ready_time]


        return self.end_task_time

    def get_finish(self):
        return self.end_task_time

    def update_ready_time(self, ready_time):
        self.ready_time = ready_time

    def get_ready_time(self):
        return self.ready_time

    def get_client_idx(self):
        return self.client_idx

    def get_priority(self):
        return self.priority


class JobClass:
    counter = 0
    ranks = [0, 1]

    def __init__(self):
        # when is job done? until last task passed the simulation time
        self.taskcollection = []
        self.current_task_idx = 0
        self.job_done = False
        self.previous_task_idx = None

    def current_task(self):
        return self.taskcollection[self.current_task_idx]

    def update_previous_task(self, task_start_time):
        self.taskcollection[self.previous_task_idx].set_start_time(task_start_time)
        # update current task ready time
        if self.previous_task_idx != self.current_task_idx:
            self.taskcollection[self.current_task_idx].ready_time = self.taskcollection[self.previous_task_idx].end_task_time

    def move_to_next_task(self, task_start_time):
        self.previous_task_idx = self.current_task_idx
        next_task_ready_time = self.taskcollection[self.current_task_idx].set_start_time(task_start_time)
        if self.current_task_idx < len(self.taskcollection) - 1:

            self.current_task_idx = self.current_task_idx + 1
            self.taskcollection[self.current_task_idx].update_ready_time(next_task_ready_time)

        else:
            self.job_done = True

    def set_job(self, key, value):
        self.job_id = key
        for v in value:
            self.taskcollection.append(TaskClass(v, key))

    def __str__(self):
        output = [f'{self.job_id}']
        for task in self.taskcollection:
            output.append(task.__str__())
        out = ('\n').join(o for o in output)
        return out

    def get_job_id(self):
        return self.job_id

    def get_next_task(self):
        # return next task to do
        if  self.job_done:
            return None
        return self.taskcollection[self.current_task_idx]


class JobCollection():
    def __init__(self):
        self.jobs = {}

    def __getitem__(self, key):
        return self.jobs[key]

    def add_job(self, job):
        self.jobs[job.get_job_id()] = job

    def show_job(self, job_key):
        print(self.jobs[job_key])

    def update_previous_task(self, job_key, task_start_time):
        self.jobs[job_key].update_previous_task(task_start_time)

    def mark_job_task_finish(self, job_key, task_start_time):
        self.jobs[job_key].move_to_next_task(task_start_time)
        return self.jobs[job_key]

    def next_task_for_each_stage(self, ends_time):

        # look the next task for each job, assign a task with smallest arrival time for that stage to the next avlaible worker
        task_to_do = {}

        for job_key, job in self.jobs.items():
            next_task = job.get_next_task()
            if next_task is None: continue
            task_stage = next_task.get_client_idx()
            if task_stage != paras[batch_stage_idx_str]:
                if next_task.ready_time + next_task.duration > ends_time: continue
                if task_stage not in task_to_do:
                    task_to_do[task_stage] = next_task_type(job_key=job_key, task=next_task)
                else:
                    if task_to_do[task_stage].task.first_task_ready_time > next_task.first_task_ready_time:
                        task_to_do[task_stage] = next_task_type(job_key=job_key, task=next_task)
            else:
                if task_stage not in task_to_do:
                    task_to_do[task_stage] = []
                task_to_do[task_stage].append(next_task_type(job_key=job_key, task=next_task))
        # print('task to do are ')
        # print(task_to_do)
        return task_to_do

    def next_task_stage(self, stage):
        task_to_do = None
        # find the job which has a task to do at stage whose min ready time is smallest
        # first find all jobs whose next ask is at this stage
        if stage != paras[batch_stage_idx_str] and stage != nigh_stage:
            for job_key, job in self.jobs.items():
                next_task = job.get_next_task()
                if next_task.client_idx == stage:
                    # print(next_task)
                    if task_to_do is not None:
                        if task_to_do.first_task_ready_time > next_task.first_task_ready_time:
                            task_to_do = next_task
                    else:
                        task_to_do = next_task
            # print(f'next task at {stage}')
            # print(task_to_do)
        else:
            # we are trying to find batched
            task_to_do = []
            for job_key, job in self.jobs.items():
                next_task = job.get_next_task()
                if next_task.client_idx == stage:
                    task_to_do.append(next_task)
            # print('to do embedding', len(task_to_do))
    def get_result(self):
        for job_key, job in self.jobs.items():
            for t in job.taskcollection:
                if t.task_finished == True:
                    paras['result'].append(t.finished_data)