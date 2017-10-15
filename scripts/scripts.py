import os
import sys

d = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(d, '../'))
sys.path.append(os.path.join(d, '../submodules/nfldb'))
sys.path.append(os.path.join(d, '../submodules/nflgame'))
