import numpy
import glob, os
from OrderingDataFrame import order
import pandas as pd



def order(frame,var):
    varlist =[w for w in frame.columns if w not in var]
    frame = frame[var+varlist]
    return frame




str4 = "*.csv"
str5 = str4
print(str5)
Filename = r"\\qafile2\Leonardo\Feature Data\ValidationUnitetest_vasista\SecondaryCharacteristics\InputFiles"
filename = r"\\qafile2\Leonardo\Feature Data\ValidationUnitetest_vasista\SecondaryCharacteristics\InputFiles.vvaz.csv"
print(Filename)
os.chdir(Filename)
files = glob.glob(str5)
files = pd.DataFrame(files)
print(files)
files.to_csv(filename, sep=',', encoding='utf-8', index=False, header= False)
