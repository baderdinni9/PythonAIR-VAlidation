import pandas as pd
import numpy
import glob, os

str1 = "*_LOSS2CSV_*.csv"
str2 = r'C:\LosstoCsvTest'
os.chdir(str2)
files = glob.glob(str1)
print(files)