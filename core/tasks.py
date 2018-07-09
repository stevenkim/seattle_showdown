from os import path

import copy
import inspect
import log
import pickle

class Task:
    '''
    An abstract class that represents a single task to complete.
    '''

    def __init__(self, id, dependencies=[], **kwargs):
        frame = inspect.stack()[2]
        module = inspect.getmodule(frame[0])
        self._full_id = module.__name__ + '.' + id
        self.dependencies = dependencies
        self.setkwargs(**kwargs)
        self._date_period_offset = 0
        self.force_run = False

    def get_full_id(self):
        return self._full_id + '(offset=%d)' % self._date_period_offset

    def setkwargs(self, **kwargs):
        raise NotImplementedError()

    def doit(self, date_period):
        raise NotImplementedError()

    def depends_on(self, *args):
        self.dependencies = list(args)
        return self

    def set_force_run(self):
        self.force_run = True

    def run(self, date_period):
        if self._date_period_offset != 0:
            date_period = date_period.offset(self._date_period_offset)

        log.info('Running Task %s for Season %d Week %d', self.get_full_id(),
            date_period.season_year, date_period.week)
        self.doit(date_period)

    def date_period_offset(self, offset):
        other = copy.copy(self)
        other._date_period_offset = offset
        return other

    def __str__(self):
         return self.get_full_id()

def pandas_task(csv):
    def wrapper(func_to_wrap):
        return PandasTask(
            id=func_to_wrap.__name__,
            func=func_to_wrap,
            csv=csv,
        )
    return wrapper


class PandasTask(Task):
    '''
    Output of the function must be a Pandas DataFrame object.
    '''
    def setkwargs(self, **kwargs):
        self.func = kwargs['func']
        self.csv = kwargs['csv']

    def doit(self, date_period):
        filepath = date_period.get_data_file(self.csv)
        if path.exists(filepath) and not self.force_run:
            log.info('File %s already exists. Skipping', filepath)
            return
        results = self.func(date_period)
        filepath = date_period.get_data_file(self.csv)
        results.to_csv(filepath)


def scikit_task(model_name):
    def wrapper(func_to_wrap):
        return ScikitTask(
            id=func_to_wrap.__name__,
            func=func_to_wrap,
            model_name=model_name,
        )
    return wrapper


class ScikitTask(Task):
    '''
    Output of the function must be a Scikit model
    '''
    def setkwargs(self, **kwargs):
        self.func = kwargs['func']
        self.model_name = kwargs['model_name']

    def doit(self, date_period):
        filepath = date_period.get_data_file(self.model_name + ".model")
        if path.exists(filepath) and not self.force_run:
            log.info('File %s already exists. Skipping', filepath)
            return
        results = self.func(date_period)
        with open(filepath, 'w') as f:
            pickle.dump(results, f)
