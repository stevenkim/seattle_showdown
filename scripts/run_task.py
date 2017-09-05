import argparse
from tasks.base import Task
from importlib import import_module

parser = argparse.ArgumentParser(description="Run some tasks")
parser.add_argument('module')
args = parser.parse_args()

task_module = args.module
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
    task.run('123')
