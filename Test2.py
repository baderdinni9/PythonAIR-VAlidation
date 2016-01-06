#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Parse Location Validation Script
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


Filename = r"\\qafile2\Leonardo\Feature Data\ImportToolValidation\InputFiles\GeocodingCentroidLL.csv" #TCID
print(Filename)


datacontractnew = pd.DataFrame()


try:
    datafile = pd.read_csv(Filename, encoding="utf-16", sep=',')
except :
    try:
        datafile = pd.read_csv(Filename, encoding="utf-8", sep=',')
    except:
        datafile = pd.read_csv(Filename, encoding="ISO-8859-1", sep=',')


data.columns = pd.Series(data.columns).str.replace(' ', '')
print(data)
