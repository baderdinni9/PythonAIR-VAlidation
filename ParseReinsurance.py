#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Parse Reinsurance Validation Script
******************************************************

"""
__author__ = 'Vasista'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Vasista'
__email__ = 'vbaderdinni@air-worldwide.com'
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


Filename = sys.argv[3] #TCID
print(Filename)


datacontractnew = pd.DataFrame()
try:
    data = pd.read_csv(Filename, encoding='utf-16', sep=',')
except:
    data = pd.read_csv(Filename, encoding='utf-8', sep=',')

data.columns = pd.Series(data.columns).str.replace(' ', '')

actualcolumns = [r'ContractID',	r'ReinsuranceID',	r'ReinsuranceType',	r'ExposureID',	r'ExposureType',	r'CededPercentage',	r'ReinsuranceLimit',	r'AttachmentAmount',	r'ReinsurerName',	r'CedantName',	r'Currency',	r'ReinsurancePerils']
columnmissing = set(actualcolumns)-set(data.columns)
print(columnmissing)
for column in columnmissing:
    data[column] = 0

datacontract = data[[r'ContractID',	r'ReinsuranceID',	r'ReinsuranceType',	r'ExposureID',	r'ExposureType',	r'CededPercentage',	r'ReinsuranceLimit',	r'AttachmentAmount',	r'ReinsurerName',	r'CedantName',	r'Currency',	r'ReinsurancePerils']]
datacontract = pd.DataFrame(datacontract)

datacontract['FileName'] = Filename
datacontract = order(datacontract , ['FileName'])
datacontractnew = datacontractnew.append(datacontract)


datacontractnew.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index=False)

datacontractnew.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)