#!/bin/bash

submodules="${BASH_SOURCE%/*}/../submodules"
PYTHONPATH=$submodules/nfldb:$submodules/nflgame python $@
