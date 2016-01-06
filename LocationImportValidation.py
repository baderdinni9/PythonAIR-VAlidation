__author__ = 'i54729'
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Contract Import Validation
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

import pandas as pd
import glob, os
import copy
import pip
import pandas.io.sql as psql
from pandas import DataFrame

def order(frame,var):
    varlist =[w for w in frame.columns if w not in var]
    frame = frame[var+varlist]
    return frame
# Import external Python libraries
try:
    import pandas as pd
except:
    pip.main(['install', 'pandas'])
    import pandas as pd
try:
    import pyodbc
except:
    pip.main(['install', 'pyodbc'])
    import pyodbc


class Database:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.connection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server)
        self.cursor = self.connection.cursor()

    def fetchData(self, resultDb, ValID, TCID, TPID):

        script = 'select * from ['+ resultDb + ']..[MastertemptableLocation_' + ValID + '_' + TCID + '_' + TPID + ']'
        #self.cursor.execute(script)
        # info = copy.deepcopy(self.cursor.fetchall())
        # print(info)
        # return info[-1][0]
        return copy.deepcopy(pd.read_sql(script, self.connection))


##########################################Inputs##############################################################
str1 = "_ExposureToCsvExport_"
TCID = sys.argv[3]
ValID = sys.argv[4]
str4 = "*LocationData*.csv"
str5 = TCID + str1 + ValID + str4
TPID = sys.argv[5]
resultdb = sys.argv[6]
print(str5)
Filename = sys.argv[7] #folder (outfile)
print(Filename)
os.chdir(Filename)
files = glob.glob(str5)
print(files)
server = sys.argv[8]





###########################################Database Manipulation#################################################


db = Database(server)
DatabaseContract = db.fetchData(resultdb, ValID, TCID, TPID)
#DatabaseContract[['LocationID']] = DatabaseContract[['LocationID']].astype(object)
DatabaseContract = DatabaseContract.convert_objects(convert_numeric=True)
DatabaseContract.columns = pd.Series(DatabaseContract.columns).map(str) + "_DB"
DatabaseContract.columns = pd.Series(DatabaseContract.columns).str.replace(' ', '')
DatabaseContract.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index=False)

print(DatabaseContract.dtypes)

##############################################File Manipulation###################################################

datafilenew = pd.DataFrame()

for file in files:
    filenew = file.replace('_'+ ValID,'')
    try:
        datafile = pd.read_csv(file, encoding='utf-16', sep=',')
    except:
        datafile = pd.read_csv(file, encoding='utf-8', sep=',')
    datafile.columns = pd.Series(datafile.columns).str.replace(' ', '')
    #datafile.ContractID = [datafile.ContractID.values[i][:-6] for i in range(len(datafile.ContractID.values))]
    #datafile[['LocationID']] = datafile[['LocationID']].astype(object)
    datafile['FileName'] = filenew
    datafile = order(datafile , ['FileName'])
    datafile.columns = pd.Series(datafile.columns).map(str) + "_File"
    datafile.columns = pd.Series(datafile.columns).str.replace(' ', '')
    datafilenew = datafilenew.append(datafile)
    print(datafile.dtypes)
    datafilenew.to_csv('c:\data3.csv', sep=',', encoding='utf-8', index=False)

    #######################################Merging File and Database Validation######################################

comparedata = pd.merge(datafilenew, DatabaseContract, left_on=['ContractID_File', 'LocationID_File'],
                       right_on=['ContractID_DB', 'LocationID_DB'], how='outer')

#comparedata.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index=False)
print(comparedata.columns)
comparedata.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)
