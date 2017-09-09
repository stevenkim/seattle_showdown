from os import path, makedirs

class DAG:
    def __init(self, tasks):
        self.tasks = tasks

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
            new_week = 17

        return DatePeriod(new_season, new_week, self.season_type)

    def get_data_file(self, filename):
        folder = path.join('data', '%d_%02d_%s' % (
            self.season_year, self.week, self.season_type))
        if not path.exists(folder):
            makedirs(folder)
        return path.join(folder, filename)

class Task:
    def __init__(self, dependencies=[], **kwargs):
        self.dependencies = dependencies
        self.setkwargs(**kwargs)

    def setkwargs(self, **kwargs):
        raise NotImplementedError()

    def doit(self):
        raise NotImplementedError()

    def run(self, task_id, date_period):
        self.task_id = task_id
        self.doit(date_period)


class NFLDBQueryTask(Task):
    def setkwargs(self, **kwargs):
        self.func = kwargs['nfldb_func']
        self.csv = kwargs['csv']

    def doit(self, date_period):
        results = self.func(date_period)
        filepath = date_period.get_data_file(self.csv)
        results.to_csv(filepath)
