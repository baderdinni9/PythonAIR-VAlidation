import pandas as pd
import numpy
import glob, os

import re

file = open(r"\\qafile2\Leonardo\Feature Data\ValidationUnitetest_vasista\LosstoCSVTool\Scripts and Documents\XML .txt", "r")

for line in file.readlines():
    with open('c:\Hazard.txt', 'a') as f:
        if re.search('<Descriptioin>', line, re.I):
            print line
            f.write(line)