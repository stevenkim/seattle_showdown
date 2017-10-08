import argparse
from core.dag import DAG, DatePeriod
from importlib import import_module

parser = argparse.ArgumentParser(description="Run some tasks")
parser.add_argument('module')
parser.add_argument('season_year', type=int)
parser.add_argument('week', type=int)
parser.add_argument('--season_type')
parser.add_argument('--fill_weeks', nargs='?', type=int, default=1)
parser.add_argument('--filter', nargs='?', type=str, default='')
parser.add_argument('--execute_dependents', type=bool, default=False)
parser.add_argument('--force_run', type=bool, default=False)
args = parser.parse_args()

task_module = args.module
season_year = int(args.season_year)
week = int(args.week)

module = import_module(task_module)
tasks_to_run = module.TASKS
filter_regex = str(args.filter)

dag = DAG(tasks_to_run, filter_regex, args.execute_dependents, args.force_run)

fill_weeks = int(args.fill_weeks)
period = DatePeriod(season_year, week)
for offset in range(0, fill_weeks):
    dag.run(period)
    period = period.offset(1)
