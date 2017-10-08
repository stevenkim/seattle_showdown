from os import path, makedirs
from sets import Set
import re
import log
import settings

class DAG:
    '''
    Directed Acylic Graph.  Basically give it a bunch of tasks and it will
    try to execute them.
    '''
    def __init__(self, tasks, filter_regex='', execute_dependents=False,
        force_run=False):

        all_tasks = {}
        def recurse(task):
            if task.get_full_id() in all_tasks:
                return
            for dep_task in task.dependencies:
                recurse(dep_task)
            all_tasks[task.get_full_id()] = task
        for task in tasks:
            recurse(task)

        self.execute_dependents = execute_dependents
        self.force_run = force_run
        self.filter_regex = filter_regex
        self.tasks = all_tasks

    def run(self, period):
        # find the leaf nodes
        downstream_tasks = {}

        tasks = self.tasks
        if self.filter_regex:
            tasks = { k: v for (k, v) in tasks.items() \
                if re.search(self.filter_regex, k)}

        # probably a dumb way to do this but whatever
        for task_id in tasks:
            task = tasks[task_id]
            for dependent_task in task.dependencies:
                if dependent_task.get_full_id() not in downstream_tasks:
                    downstream_tasks[dependent_task.get_full_id()] = Set()
                downstream_tasks[dependent_task.get_full_id()].add(task.get_full_id())

        leaf_tasks = []
        for task_id in tasks:
            task = tasks[task_id]
            if task.get_full_id() not in downstream_tasks:
                leaf_tasks.append(task)

        tasks_to_execute = leaf_tasks
        executed_tasks = Set()
        def recurse(task):
            if task.get_full_id() in executed_tasks:
                return

            executed_tasks.add(task.get_full_id())
            for dependent_task in task.dependencies:
                if self.execute_dependents:
                    recurse(dependent_task)
                else:
                    log.info('skipping %s', dependent_task.get_full_id())
            if self.force_run:
                task.set_force_run()
            task.run(period)
        for task_to_execute in tasks_to_execute:
            recurse(task_to_execute)


class DatePeriod:
    '''
    A unit of time to execute a DAG against.  In our case it is the week and
    the year of the football season
    '''
    def __init__(self, season_year, week, season_type='Regular'):
        self.season_year = season_year
        self.week = week
        self.season_type = season_type

    def offset(self, number_of_weeks):
        new_week = self.week + number_of_weeks
        new_season = self.season_year
        if new_week > 17:
            new_season += 1
            new_week = 1
        elif new_week < 1:
            new_season -= 1
            # this is for fantasy football so regular season only matters
            new_week = 17

        return DatePeriod(new_season, new_week, self.season_type)

    def get_data_file(self, filename):
        folder = path.join(settings.data_dir, '%d_%02d_%s' % (
            self.season_year, self.week, self.season_type))
        if not path.exists(folder):
            makedirs(folder)
        return path.join(folder, filename)
