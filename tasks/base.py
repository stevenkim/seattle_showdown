from os import path, makedirs
from sets import Set
import copy
import datetime
import inspect

DEBUG = True
def debug(fmt, *args):
    if not DEBUG:
        return

    print('['+str(datetime.datetime.now())+'][DEBUG] '+(fmt % args))

def info(fmt, *args):
    print('['+str(datetime.datetime.now())+'][INFO] '+(fmt % args))

class DAG:
    def __init__(self, tasks, filter_regex=''):
        self.tasks = tasks
        self.filter_regex = filter_regex

    def run(self, period):
        # find the leaf nodes
        downstream_tasks = {}

        # probably a dumb way to do this but whatever
        for task in self.tasks:
            for dependent_task in task.dependencies:
                if dependent_task.get_full_id() not in downstream_tasks:
                    downstream_tasks[dependent_task.get_full_id()] = Set()
                downstream_tasks[dependent_task.get_full_id()].add(task.get_full_id())

        leaf_tasks = []
        for task in self.tasks:
            if task.get_full_id() not in downstream_tasks:
                leaf_tasks.append(task)

        tasks_to_execute = leaf_tasks
        executed_tasks = Set()
        def recurse(task):
            if task.get_full_id() in executed_tasks:
                return
            for dependent_task in task.dependencies:
                recurse(dependent_task)
            task.run(period)
        for task_to_execute in tasks_to_execute:
            recurse(task_to_execute)


class DatePeriod:
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
        folder = path.join('data', '%d_%02d_%s' % (
            self.season_year, self.week, self.season_type))
        if not path.exists(folder):
            makedirs(folder)
        return path.join(folder, filename)

class Task:
    def __init__(self, id, dependencies=[], **kwargs):
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        self._full_id = module.__name__ + '.' + id
        self.dependencies = dependencies
        self.setkwargs(**kwargs)
        self._date_period_offset = 0

    def get_full_id(self):
        return self._full_id + '(offset=%d)' % self._date_period_offset

    def setkwargs(self, **kwargs):
        raise NotImplementedError()

    def doit(self, date_period):
        raise NotImplementedError()

    def run(self, date_period):
        if self._date_period_offset != 0:
            date_period = date_period.offset(self._date_period_offset)

        info('Running Task %s for Season %d Week %d', self.get_full_id(),
            date_period.season_year, date_period.week)
        self.doit(date_period)

    def date_period_offset(self, offset):
        other = copy.copy(self)
        other._date_period_offset = offset
        return other


class NFLDBQueryTask(Task):
    def setkwargs(self, **kwargs):
        self.func = kwargs['nfldb_func']
        self.csv = kwargs['csv']

    def doit(self, date_period):
        results = self.func(date_period)
        filepath = date_period.get_data_file(self.csv)
        results.to_csv(filepath)
