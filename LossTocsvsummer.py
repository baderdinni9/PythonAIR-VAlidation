#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Detailed Loss to CSV Validation Script
******************************************************

"""
__author__ = 'Vasista'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Vasista'
__status__ = 'Complete'

print("dsafas")

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


str1 = "_LOSS2CSV_"
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
print(files)

datasumnew = pd.DataFrame()

for file in files:
    filenew = file.replace('_'+ ValID,'')
    data = pd.read_csv(file, encoding='utf-16', sep=',')

    data.columns = pd.Series(data.columns).str.replace(' ', '')
    if r'CatalogTypeCode' in data.columns : data['CatalogTypeCode'] = 1
    
    datasum = data[[r'CatalogTypeCode',	r'GUPLoss',	r'GUPLoss_SD',	r'GUPLoss_Mod',	r'GUPLoss_ModSD',	r'RTLoss',	r'RTLoss_SD',	r'RTLoss_Mod',	r'RTLoss_ModSD',	r'GRLoss',	r'GRLoss_SD',	r'GRLoss_Mod',	r'GRLoss_ModSD',	r'PreCATNetLoss',	r'PreCATNetLoss_Mod',	r'PostCATNetLoss',	r'PostCATNetLoss_Mod',	r'PreLayerGrossLoss',	r'PreLayerGrossLoss_SD',	r'PreLayerGrossLoss_Mod',	r'PreLayerGrossLoss_ModSD',	r'GUPLoss_A',	r'GUPLoss_A_Mod',	r'GUPLoss_B',	r'GUPLoss_B_Mod',	r'GUPLoss_C',	r'GUPLoss_C_Mod',	r'GUPLoss_D',	r'GUPLoss_D_Mod',	r'GRLoss_A',	r'GRLoss_A_Mod',	r'GRLoss_B',	r'GRLoss_B_Mod',	r'GRLoss_C',	r'GRLoss_C_Mod',	r'GRLoss_D',	r'GRLoss_D_Mod',	r'NTLoss_A',	r'NTLoss_A_Mod',	r'NTLoss_B',	r'NTLoss_B_Mod',	r'NTLoss_C',	r'NTLoss_C_Mod',	r'NTLoss_D',	r'NTLoss_D_Mod',	r'PreLayerGrossLoss_A',	r'PreLayerGrossLoss_B',	r'PreLayerGrossLoss_B_Mod',	r'PreLayerGrossLoss_C',	r'PreLayerGrossLoss_C_Mod',	r'PreLayerGrossLoss_D',	r'PreLayerGrossLoss_D_Mod',	r'GUPLoss_Minor',	r'GUPLoss_Moderate',	r'GUPLoss_Major',	r'GUPLoss_Fatal',	r'GRLoss_Minor',	r'GRLoss_Moderate',	r'GRLoss_Major',	r'GRLoss_Fatal',	r'NTLoss_Minor',	r'NTLoss_Moderate',	r'NTLoss_Major',	r'NTLoss_Fatal']].sum()
    datasum = pd.DataFrame(datasum).transpose()
    datasum['FileName'] = filenew
    datasum['NumberofRows'] = len(data)
    datasum = order(datasum , ['FileName'])
    datasumnew = datasumnew.append(datasum)


#datasumnew.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index=False)

datasumnew.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)
