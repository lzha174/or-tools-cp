from commonstr import *

class WorkerClass:
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        self.next_avalaible_time = self.start_time

    def update_avaliable_time(self, task_start_time, time):
        self.next_avalaible_time = task_start_time + time

    def get_avaliable_time(self):
        return self.next_avalaible_time

    def __str__(self):
        f = f'woker start {self.start_time} end {self.end_time} avaliabe {self.next_avalaible_time}'
        return f

class WokrerCollection:
    def __init__(self, stage, nb_worker, start_time, end_time):
        self.workers = [WorkerClass(start_time, end_time) for i in range(nb_worker)]
        self.stage = stage
        self.start = format_time(start_time)
        self.end = format_time(end_time)
    
    def __str__(self):
        output = [f'stage {self.stage} with {len(self.workers)} workers start {self.start} end {self.end}']
        if self.stage != paras[batch_stage_idx_str] and self.stage != nigh_stage:
            for w in self.workers:
                output.append(w.__str__())
    
        return ('\n').join(o for o in output)

    def next_avaliable_worker(self, ready_time):
        next_avalible_time = 1e9
        next_worker = None
        for worker in self.workers:
            #if worker.get_avaliable_time() < ready_time: continue
            if worker.get_avaliable_time() < next_avalible_time :
                next_avalible_time = worker.get_avaliable_time()
                next_worker = worker
        return next_worker

class TaskClass:
    def __init__(self, task):
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

    def __str__(self):
        stage = self.task.client_idx
        client = paras[idx_to_name_client_str][stage]
        return (f'task {client} start {format_time(self.start_task_time)} end {format_time(self.end_task_time)} finished {self.task_finished} ready {format_time(self.ready_time)}')

    def get_task(self):
        return self.task

    def get_first_task_ready_time(self):
        return self.first_task_ready_time


    def set_finish(self, time):
        self.task_finished = True
        self.task_finish_time = time

    def set_start_time(self, task_start_time):
        self.start_task_time = task_start_time
        self.end_task_time = task_start_time + self.duration
        self.task_finished = True
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

    def move_to_next_task(self, task_start_time):

        next_task_ready_time = self.taskcollection[self.current_task_idx].set_start_time(task_start_time)
        if self.current_task_idx < len(self.taskcollection) - 1:

            self.current_task_idx = self.current_task_idx + 1
            self.taskcollection[self.current_task_idx].update_ready_time(next_task_ready_time)

        else:
            self.job_done = True

    def set_job(self, key, value):
        self.job_id = key
        for v in value:
            self.taskcollection.append(TaskClass(v))

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
        return self.taskcollection[self.current_task_idx]


class JobCollection():
    def __init__(self):
        self.jobs = {}

    def add_job(self, job):
        self.jobs[job.get_job_id()] = job
        return job.__str__()

    def show_job(self, job_key):
        print(self.jobs[job_key])

    def mark_job_task_finish(self, job_key, task_start_time):
        self.jobs[job_key].move_to_next_task(task_start_time)

    def next_task_for_each_stage(self, ends_time):

        # look the next task for each job, assign a task with smallest arrival time for that stage to the next avlaible worker
        task_to_do = {}

        for job_key, job in self.jobs.items():
            next_task = job.get_next_task()
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

        print(task_to_do)
        return task_to_do

    def next_task_stage(self, stage):
        task_to_do = None
        # find the job which has a task to do at stage whose min ready time is smallest
        # first find all jobs whose next ask is at this stage
        if stage != paras[batch_stage_idx_str] and stage != nigh_stage:
            for job_key, job in self.jobs.items():
                next_task = job.get_next_task()
                if next_task.client_idx == stage:
                    print(next_task)
                    if task_to_do is not None:
                        if task_to_do.first_task_ready_time > next_task.first_task_ready_time:
                            task_to_do = next_task
                    else:
                        task_to_do = next_task
            print(f'next task at {stage}')
            print(task_to_do)
        else:
            # we are trying to find batched
            task_to_do = []
            for job_key, job in self.jobs.items():
                next_task = job.get_next_task()
                if next_task.client_idx == stage:
                    task_to_do.append(next_task)
            print('to do embedding', len(task_to_do))
