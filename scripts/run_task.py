import argparse
from tasks.base import DatePeriod, Task
from importlib import import_module

parser = argparse.ArgumentParser(description="Run some tasks")
parser.add_argument('module')
parser.add_argument('season_year', type=int)
parser.add_argument('week', type=int)
parser.add_argument('--season_type')
parser.add_argument('--fill_weeks', nargs='?', type=int, default=1)
args = parser.parse_args()

task_module = args.module
season_year = int(args.season_year)
week = int(args.week)

period = DatePeriod(season_year, week)

module = import_module(task_module)

# find all task objects
tasks_to_run = {}
for attr in dir(module):
    maybe_task = getattr(module, attr)
    if isinstance(maybe_task, Task):
        tasks_to_run[task_module + "." + attr] = maybe_task

fill_weeks = int(args.fill_weeks)

for task_name in tasks_to_run:
    task = tasks_to_run[task_name]

    for offset in range(0, fill_weeks):
        print("Running %s for Season %d Week %d" %
            (task_name, period.season_year, period.week))
        task.run(task_name, period)
        period = period.offset(1)
