#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Geospatial Loss to CSV Validation Script
******************************************************

"""
__author__ = 'Vasista'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Vasista'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'



# Import standard Python packages and read outfile
import getopt
import sys
import datetime
import warnings
import pandas as pd

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)

if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import multiprocessing as mp
import glob, os


import numpy
import glob, os



def order(frame,var):
    varlist =[w for w in frame.columns if w not in var]
    frame = frame[var+varlist]
    return frame


str1 = "_Geospatial2CSV_"
TCID = sys.argv[3] #TCID
print(TCID)

ValID = sys.argv[4] #ValID
print(ValID)
str4 = "*.csv"
str5 = TCID + str1 + ValID + str4
print(str5)
Filename = sys.argv[5] #folder (outfile)
print(Filename)
os.chdir(Filename)
files = glob.glob(str5)


datasumnew = pd.DataFrame()

for file in files:
    filenew = file.replace('_'+ ValID,'')
    try:
        data = pd.read_csv(file, encoding='utf-16', sep=',')
    except:
        data = pd.read_csv(file, encoding='utf-8', sep=',')

    data.columns = pd.Series(data.columns).str.replace(' ', '')

    actualcolumns = ['TotalRepValue','RiskCount','LocationCount','GUPLoss','GRLoss','PreCATNetLoss','PostCATNetLoss']
    columnmissing = set(actualcolumns)-set(data.columns)
    print(columnmissing)
    for column in columnmissing:
            data[column] = 0

    datasum = data[[r'TotalRepValue', r'RiskCount', r'LocationCount', r'GUPLoss', r'GRLoss', r'PreCATNetLoss', r'PostCATNetLoss']].sum()
    datasum = pd.DataFrame(datasum).transpose()

    datasum['FileName'] = filenew
    datasum['NumberofRows'] = len(data)
    datasum = order(datasum , ['FileName'])
    datasumnew = datasumnew.append(datasum)


#datasumnew.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index=False)

datasumnew.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)