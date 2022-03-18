import pandas as pd

from initial_paras import *
#model lunch break propery for the heuristic model
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
    def __init__(self, idx, day, period, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time, stage):
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
        self.total_tasks_assgined = 0
        self.interval_modify = None
        self.stage =  stage
        self.breaks = []
        self.finished_shift = False
        self.mid_day = mid_day_time
            # insert the lunch as a fake task, this may create idle interval before  lunch, marke the last finish task as lunch finish task
        if lunch_start_time >= start_time and lunch_end_time <= end_time:

            self.breaks.append(pd.Interval(left = lunch_start_time, right = lunch_end_time, closed = 'left'))


    def find_start_time_with_breaks(self, task_start_time, duration):
        task_inteval = pd.Interval(left = task_start_time, right = task_start_time + duration)
        for w_break in self.breaks:
            if task_inteval.overlaps(w_break):
                # cant do this task in this shift
                if  w_break.right + duration >= self.end_time:
                    return  None
                task_inteval = pd.Interval(left=w_break.right, right= w_break.right + duration)
            else:
                break
        # if we cant find any time to start this task, this worker cant do any work any more in this shift?
        if task_inteval.right < self.end_time:
            return task_inteval.left
        return None

    def free_to_next_task(self, task):
        if task.ready_time >= self.end_time:
            return False, None, None
        task_start_time = custom_max(self.last_task_finish_time, task.ready_time)
        adjusted_start_time = self.find_start_time_with_breaks(task_start_time, task.duration)

        if adjusted_start_time is None:
            return False, None, None
        adjusted_end_time = adjusted_start_time + task.duration
        return True, adjusted_start_time, adjusted_end_time

    def update_free_time(self, start_time, end_time, task):
        assert (end_time < self.end_time)
        self.last_task_finish_time = end_time

        self.update_gantt(start_time, task)

    def check_overlap(self):
        nb = len(self.task_gantt)
        for i in range(nb - 1):
            j = i + 1
            task_i_end = self.task_gantt[i][4] + self.task_gantt[i][5]
            task_j_start = self.task_gantt[j][4]
            assert(task_i_end <= task_j_start)


    def update_gantt(self, task_start_time, task):
        task_gantt = [self.day, self.period, self.stage, self.idx, task_start_time, task.duration]
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

    def __lt__(self, other):
        return self.last_task_finish_time < other.last_task_finish_time

class BatcherWorkerClass (WorkerClass):
    def __init__(self, idx, day, period, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time, stage):
        WorkerClass.__init__(self, idx, day, period, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time, stage)
        self.duration = end_time - start_time
        self.isIdle = True

    def is_idle(self):
        return self.isIdle

    def fill_task(self):
        self.isIdle = False
        self.last_task_finish_time = self.end_time
        return self.end_time



class WokrerCollection:
    def __init__(self, day, period, stage, nb_worker, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time):
        self.workers = [WorkerClass(i, day, period, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time, stage) for i
                        in range(nb_worker)]
        self.stage = stage
        self.start = format_time(start_time)
        self.end = format_time(end_time)
        self.day = day
        self.period = period

    def sort_workers(self):
        self.workers = sorted(self.workers)

    def earliest_free_worker(self):
        # sorted list by free time
        return self.workers[0]

    def check_overlap(self):
        for w in self.workers:
            w.check_overlap()

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


class BatchCollection (WokrerCollection):
    def __init__(self,  day, period, stage, nb_worker, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time):
        WokrerCollection.__init__(self, day, period, stage, nb_worker, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time)
        self.workers = [BatcherWorkerClass(i, day, period, start_time, end_time, mid_day_time, lunch_start_time, lunch_end_time, stage) for i
                        in range(nb_worker)]

    def free_count(self):
        count = 0
        for w in self.workers:
            if w.last_task_finish_time == w.start_time:
                count = count + 1
        return count


    def get_idle_batch_worker(self, task):
        for w in self.workers:
            if w.is_idle() and task.ready_time < w.start_time:
                return w
        return None



class TaskClass:

    def __init__(self, task, job_id=0):
        self.task = task
        self.start_task_time = 0
        self.duration = task.duration
        self.end_task_time = 0
        self.task_finished = False
        self.ready_time = task.ready_time
        self.client_idx = task.client_idx
        self.priority = task.priority_idx
        self.first_task_ready_time = task.first_task_ready_time
        self.job_id = job_id
        self.task_rank = 0

    def __str__(self):
        stage = self.task.client_idx
        client = paras[idx_to_name_client_str][stage]
        return (
            f'job {self.job_id} task {client} start {format_time(self.start_task_time)} end {format_time(self.end_task_time)} priority ={self.priority} duration {self.duration} ready {format_time(self.ready_time)}')


    def get_task(self):
        return self.task

    def get_first_task_ready_time(self):
        return self.first_task_ready_time

    def set_finish(self, time):
        self.task_finished = True

    def set_start_time(self, task_start_time = 0, duration = None):
        self.start_task_time = task_start_time
        if duration != None:
            self.end_task_time = task_start_time + duration
        else:
            self.end_task_time = task_start_time + self.duration
        self.task_finished = True

        start_time = format_time(self.start_task_time)
        end_time = format_time(self.end_task_time)
        ready_time = format_time(self.ready_time)

        task_name = paras["idx_to_name_client"][self.task.client_idx]

        case_key_idx = self.task.case_key_idx
        case_name = paras[idx_to_name_key_str][case_key_idx]
        priority = paras[idx_to_name_priority_str][self.priority]
        finished_data = [case_key_idx, case_name, priority, task_name, self.task.order, start_time, end_time,
                         self.duration, ready_time]
        paras['result'].append(finished_data)

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

    def __init__(self):

        self.taskcollection = []
        self.current_task_idx = 0
        self.job_done = False
        self.batch_count = 0

    def check_task(self):
        for i in range(len(self.taskcollection) - 1):
            if self.taskcollection[i].task_finished and self.taskcollection[i + 1].task_finished:
                assert(self.taskcollection[i].end_task_time <= self.taskcollection[i + 1].start_task_time)

    def reduce_batch_count(self):
        self.batch_count = self.batch_count - 1
        assert(self.batch_count >= 0)

    def move_to_next_task(self, task_start_time = None, duration = None):
        assert(task_start_time != None)

        next_task_ready_time = self.taskcollection[self.current_task_idx].set_start_time(task_start_time, duration)


        if self.current_task_idx < len(self.taskcollection) - 1:

            self.current_task_idx = self.current_task_idx + 1
            self.taskcollection[self.current_task_idx].update_ready_time(next_task_ready_time)

        else:
            self.job_done = True

    def set_job(self, key, embedding, value):
        self.job_id = key
        self.batch_count = embedding
        for v in value:
            self.taskcollection.append(TaskClass(v, key))

    def __str__(self):
        output = [f'{self.job_id} embedding {self.batch_count}']
        for task in self.taskcollection:
            output.append(task.__str__())
        output.append(f'current {self.taskcollection[self.current_task_idx].__str__()}')
        out = ('\n').join(o for o in output)
        return out

    def get_job_id(self):
        return self.job_id

    def current_task(self):
        # return next task to do
        if self.job_done: return None
        return self.taskcollection[self.current_task_idx]


class JobCollection():
    def __init__(self):
        self.jobs = {}
    
    def __getitem__(self, item):
        return self.jobs[item]

    def add_job(self, job):
        self.jobs[job.get_job_id()] = job

    def show_job(self, job_key):
        print(self.jobs[job_key])

    def check_job(self):
        for key, job in self.jobs.items():
            job.check_task()

