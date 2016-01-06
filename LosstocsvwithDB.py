#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Detailed Loss to CSV
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

print("dsafas")

# Import standard Python packages and read outfile

import pandas as pd
import glob, os
import copy
import pip
import pandas.io.sql as psql
from pandas import DataFrame
print("dsafas")




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

        script = 'select * from ['+ resultDb + ']..[MastertemptableLossDB_' + ValID + '_1_' + TCID + '_' + TPID + ']'
        #self.cursor.execute(script)
        # info = copy.deepcopy(self.cursor.fetchall())
        print(script)
        # return info[-1][0]
        return copy.deepcopy(pd.read_sql(script, self.connection))





str1 = "_LOSS2CSV_"
TCID = sys.argv[3] #TCID
print(TCID)

ValID = sys.argv[4] #ValID
print(ValID)
str4 = "*.csv"
str5 = TCID + str1 + ValID + str4
print(str5)
TPID = sys.argv[5]
Filename = sys.argv[6] #folder (outfile)
print(Filename)
os.chdir(Filename)
files = glob.glob(str5)
print(files)
resultdb = sys.argv[7]
server = sys.argv[8]






###########################################Database Manipulation#################################################


db = Database(server)
DatabaseLossresult = db.fetchData(resultdb, ValID, TCID, TPID)
print(DatabaseLossresult)
#DatabaseContract[['LayerID']] = DatabaseContract[['LayerID']].astype(object)
DatabaseLossresult.columns = pd.Series(DatabaseLossresult.columns).map(str) + "_DB"
DatabaseLossresult.columns = pd.Series(DatabaseLossresult.columns).str.replace(' ', '')
DatabaseLossresult.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index=False)
print(DatabaseLossresult.dtypes)



##############################################File Manipulation###################################################
datasumnew = pd.DataFrame()

for file in files:
    filenew = file.replace('_'+ ValID,'')
    data = pd.read_csv(file, encoding='utf-16', sep=',')

    data.columns = pd.Series(data.columns).str.replace(' ', '')
    if r'CatalogTypeCode' in data.columns : data['CatalogTypeCode'] = 1
    if r'CatalogTypeCode' not in data.columns : data['CatalogTypeCode'] = 0
    if r'GUPLoss' not in data.columns : data['GUPLoss'] = 0
    if r'GUPLoss_SD' not in data.columns : data['GUPLoss_SD'] = 0
    if r'GUPLoss_Mod' not in data.columns : data['GUPLoss_Mod'] = 0
    if r'GUPLoss_ModSD' not in data.columns : data['GUPLoss_ModSD'] = 0
    if r'RTLoss' not in data.columns : data['RTLoss'] = 0
    if r'RTLoss_SD' not in data.columns : data['RTLoss_SD'] = 0
    if r'RTLoss_Mod' not in data.columns : data['RTLoss_Mod'] = 0
    if r'RTLoss_ModSD' not in data.columns : data['RTLoss_ModSD'] = 0
    if r'GRLoss' not in data.columns : data['GRLoss'] = 0
    if r'GRLoss_SD' not in data.columns : data['GRLoss_SD'] = 0
    if r'GRLoss_Mod' not in data.columns : data['GRLoss_Mod'] = 0
    if r'GRLoss_ModSD' not in data.columns : data['GRLoss_ModSD'] = 0
    if r'PreCATNetLoss' not in data.columns : data['PreCATNetLoss'] = 0
    if r'PreCATNetLoss_Mod' not in data.columns : data['PreCATNetLoss_Mod'] = 0
    if r'PostCATNetLoss' not in data.columns : data['PostCATNetLoss'] = 0
    if r'PostCATNetLoss_Mod' not in data.columns : data['PostCATNetLoss_Mod'] = 0
    if r'PreLayerGrossLoss' not in data.columns : data['PreLayerGrossLoss'] = 0
    if r'PreLayerGrossLoss_SD' not in data.columns : data['PreLayerGrossLoss_SD'] = 0
    if r'PreLayerGrossLoss_Mod' not in data.columns : data['PreLayerGrossLoss_Mod'] = 0
    if r'PreLayerGrossLoss_ModSD' not in data.columns : data['PreLayerGrossLoss_ModSD'] = 0
    if r'GUPLoss_A' not in data.columns : data['GUPLoss_A'] = 0
    if r'GUPLoss_A_Mod' not in data.columns : data['GUPLoss_A_Mod'] = 0
    if r'GUPLoss_B' not in data.columns : data['GUPLoss_B'] = 0
    if r'GUPLoss_B_Mod' not in data.columns : data['GUPLoss_B_Mod'] = 0
    if r'GUPLoss_C' not in data.columns : data['GUPLoss_C'] = 0
    if r'GUPLoss_C_Mod' not in data.columns : data['GUPLoss_C_Mod'] = 0
    if r'GUPLoss_D' not in data.columns : data['GUPLoss_D'] = 0
    if r'GUPLoss_D_Mod' not in data.columns : data['GUPLoss_D_Mod'] = 0
    if r'GRLoss_A' not in data.columns : data['GRLoss_A'] = 0
    if r'GRLoss_A_Mod' not in data.columns : data['GRLoss_A_Mod'] = 0
    if r'GRLoss_B' not in data.columns : data['GRLoss_B'] = 0
    if r'GRLoss_B_Mod' not in data.columns : data['GRLoss_B_Mod'] = 0
    if r'GRLoss_C' not in data.columns : data['GRLoss_C'] = 0
    if r'GRLoss_C_Mod' not in data.columns : data['GRLoss_C_Mod'] = 0
    if r'GRLoss_D' not in data.columns : data['GRLoss_D'] = 0
    if r'GRLoss_D_Mod' not in data.columns : data['GRLoss_D_Mod'] = 0
    if r'NTLoss_A' not in data.columns : data['NTLoss_A'] = 0
    if r'NTLoss_A_Mod' not in data.columns : data['NTLoss_A_Mod'] = 0
    if r'NTLoss_B' not in data.columns : data['NTLoss_B'] = 0
    if r'NTLoss_B_Mod' not in data.columns : data['NTLoss_B_Mod'] = 0
    if r'NTLoss_C' not in data.columns : data['NTLoss_C'] = 0
    if r'NTLoss_C_Mod' not in data.columns : data['NTLoss_C_Mod'] = 0
    if r'NTLoss_D' not in data.columns : data['NTLoss_D'] = 0
    if r'NTLoss_D_Mod' not in data.columns : data['NTLoss_D_Mod'] = 0
    if r'PreLayerGrossLoss_A' not in data.columns : data['PreLayerGrossLoss_A'] = 0
    if r'PreLayerGrossLoss_B' not in data.columns : data['PreLayerGrossLoss_B'] = 0
    if r'PreLayerGrossLoss_B_Mod' not in data.columns : data['PreLayerGrossLoss_B_Mod'] = 0
    if r'PreLayerGrossLoss_C' not in data.columns : data['PreLayerGrossLoss_C'] = 0
    if r'PreLayerGrossLoss_C_Mod' not in data.columns : data['PreLayerGrossLoss_C_Mod'] = 0
    if r'PreLayerGrossLoss_D' not in data.columns : data['PreLayerGrossLoss_D'] = 0
    if r'PreLayerGrossLoss_D_Mod' not in data.columns : data['PreLayerGrossLoss_D_Mod'] = 0
    if r'GUPLoss_Minor' not in data.columns : data['GUPLoss_Minor'] = 0
    if r'GUPLoss_Moderate' not in data.columns : data['GUPLoss_Moderate'] = 0
    if r'GUPLoss_Major' not in data.columns : data['GUPLoss_Major'] = 0
    if r'GUPLoss_Fatal' not in data.columns : data['GUPLoss_Fatal'] = 0
    if r'GRLoss_Minor' not in data.columns : data['GRLoss_Minor'] = 0
    if r'GRLoss_Moderate' not in data.columns : data['GRLoss_Moderate'] = 0
    if r'GRLoss_Major' not in data.columns : data['GRLoss_Major'] = 0
    if r'GRLoss_Fatal' not in data.columns : data['GRLoss_Fatal'] = 0
    if r'NTLoss_Minor' not in data.columns : data['NTLoss_Minor'] = 0
    if r'NTLoss_Moderate' not in data.columns : data['NTLoss_Moderate'] = 0
    if r'NTLoss_Major' not in data.columns : data['NTLoss_Major'] = 0
    if r'NTLoss_Fatal' not in data.columns : data['NTLoss_Fatal'] = 0

    datasum = data[[r'CatalogTypeCode',	r'GUPLoss',	r'GUPLoss_SD',	r'GUPLoss_Mod',	r'GUPLoss_ModSD',	r'RTLoss',	r'RTLoss_SD',	r'RTLoss_Mod',	r'RTLoss_ModSD',	r'GRLoss',	r'GRLoss_SD',	r'GRLoss_Mod',	r'GRLoss_ModSD',	r'PreCATNetLoss',	r'PreCATNetLoss_Mod',	r'PostCATNetLoss',	r'PostCATNetLoss_Mod',	r'PreLayerGrossLoss',	r'PreLayerGrossLoss_SD',	r'PreLayerGrossLoss_Mod',	r'PreLayerGrossLoss_ModSD',	r'GUPLoss_A',	r'GUPLoss_A_Mod',	r'GUPLoss_B',	r'GUPLoss_B_Mod',	r'GUPLoss_C',	r'GUPLoss_C_Mod',	r'GUPLoss_D',	r'GUPLoss_D_Mod',	r'GRLoss_A',	r'GRLoss_A_Mod',	r'GRLoss_B',	r'GRLoss_B_Mod',	r'GRLoss_C',	r'GRLoss_C_Mod',	r'GRLoss_D',	r'GRLoss_D_Mod',	r'NTLoss_A',	r'NTLoss_A_Mod',	r'NTLoss_B',	r'NTLoss_B_Mod',	r'NTLoss_C',	r'NTLoss_C_Mod',	r'NTLoss_D',	r'NTLoss_D_Mod',	r'PreLayerGrossLoss_A',	r'PreLayerGrossLoss_B',	r'PreLayerGrossLoss_B_Mod',	r'PreLayerGrossLoss_C',	r'PreLayerGrossLoss_C_Mod',	r'PreLayerGrossLoss_D',	r'PreLayerGrossLoss_D_Mod',	r'GUPLoss_Minor',	r'GUPLoss_Moderate',	r'GUPLoss_Major',	r'GUPLoss_Fatal',	r'GRLoss_Minor',	r'GRLoss_Moderate',	r'GRLoss_Major',	r'GRLoss_Fatal',	r'NTLoss_Minor',	r'NTLoss_Moderate',	r'NTLoss_Major',	r'NTLoss_Fatal']].sum()
    datasum = pd.DataFrame(datasum).transpose()
    datasum['FileName'] = filenew
    datasum['NumberofRows'] = len(data)
    datasum = order(datasum , ['FileName'])
    datasum.columns = pd.Series(datasum.columns).map(str) + "_File"
    datasum.columns = pd.Series(datasum.columns).str.replace(' ', '')
    datasumnew = datasumnew.append(datasum)


# print(datasumnew)
# print(datasumnew.dtypes)
datasumnew.to_csv('c:\data3.csv', sep=',', encoding='utf-8', index=False)



print(DatabaseLossresult)
#losstocsvcompare = pd.merge(datasumnew, DatabaseLossresult, how='outer', ignore_index=True)

#datasumnew = datasumnew.reset_index()
#DatabaseLossresult = DatabaseLossresult.reset_index()
losstocsvcompare = pd.merge(pd.DataFrame(datasumnew), pd.DataFrame(DatabaseLossresult), how='outer' , left_index=True,right_index=True)

losstocsvcompare.to_csv('c:\data4.csv', sep=',', encoding='utf-8', index=False)



losstocsvcompare.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)