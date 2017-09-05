class Task:
    def __init__(self, dependencies=[], **kwargs):
        self.dependencies = dependencies
        self.setkwargs(**kwargs)

    def setkwargs(self, **kwargs):
        raise NotImplementedError()

    def doit(self):
        raise NotImplementedError()

    def run(self, period_id):
        self.doit()


class NFLDBQueryTask(Task):
    def setkwargs(self, **kwargs):
        self.func = kwargs['nfldb_func']

    def doit(self):
        results = self.func()
