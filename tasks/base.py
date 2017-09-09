from os import path, makedirs

CONTEXT = {}

def set_context(context):
    global CONTEXT
    assert context['season_year']
    assert context['week']
    assert context['season_type']
    assert len(CONTEXT) == 0, 'don\'t call this twice'
    CONTEXT = context

class Task:
    def __init__(self, dependencies=[], **kwargs):
        self.dependencies = dependencies
        self.setkwargs(**kwargs)

    def setkwargs(self, **kwargs):
        raise NotImplementedError()

    def doit(self):
        raise NotImplementedError()

    def run(self, task_id, season_year, week, season_type='Regular'):
        self.task_id = task_id
        self.season_year = season_year
        self.week = week
        self.season_type = season_type
        self.doit()

    def _get_data_file(self, filename):
        folder = path.join('data', '%d_%02d_%s' % (
            self.season_year, self.week, self.season_type))
        if not path.exists(folder):
            makedirs(folder)
        return path.join(folder, '%s_%s' % (self.task_id, filename))


class NFLDBQueryTask(Task):
    def setkwargs(self, **kwargs):
        self.func = kwargs['nfldb_func']
        self.csv = kwargs['csv']

    def doit(self):
        results = self.func()
        filepath = self._get_data_file(self.csv)
        results.to_csv(filepath)
