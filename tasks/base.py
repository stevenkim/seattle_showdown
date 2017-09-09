from os import path, makedirs

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

    def _get_data_file(self, filename, date_period):
        folder = path.join('data', '%d_%02d_%s' % (
            date_period.season_year, date_period.week, date_period.season_type))
        if not path.exists(folder):
            makedirs(folder)
        return path.join(folder, '%s_%s' % (self.task_id, filename))


class NFLDBQueryTask(Task):
    def setkwargs(self, **kwargs):
        self.func = kwargs['nfldb_func']
        self.csv = kwargs['csv']

    def doit(self, date_period):
        results = self.func(date_period)
        filepath = self._get_data_file(self.csv, date_period)
        results.to_csv(filepath)
