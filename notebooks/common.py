import pandas as pd
import sys
sys.path.append('../')
sys.path.append('../submodules/nfldb')

import os
import tasks.base as base
import tasks.dvoa_crap as dvoa
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')
matplotlib.rcParams['figure.figsize'] = (16.0, 8.0)
