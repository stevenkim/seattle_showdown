import argparse
from tasks.base import set_context, Task
from importlib import import_module

parser = argparse.ArgumentParser(description="Run some tasks")
parser.add_argument('module')
parser.add_argument('season_year', type=int)
parser.add_argument('week', type=int)
parser.add_argument('--season_type')
args = parser.parse_args()

task_module = args.module
season_year = int(args.season_year)
week = int(args.week)

set_context({
    'season_year': season_year,
    'season_type': 'Regular',
    'week': week,
})

module = import_module(task_module)

# find all task objects
tasks_to_run = {}
for attr in dir(module):
    maybe_task = getattr(module, attr)
    if isinstance(maybe_task, Task):
        tasks_to_run[task_module + "." + attr] = maybe_task

for task_name in tasks_to_run:
    task = tasks_to_run[task_name]
    print("Running %s" % task_name)
    task.run(task_name, season_year=season_year, week=week)
