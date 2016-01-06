import pandas as pd
import numpy
import glob, os

str1 = "*_LOSS2CSV_*.csv"


os.chdir(r'\\qafile2\Leonardo\Feature Data\loss to csv narendra\TestFolder')
files = glob.glob(str1)
print(files)

datasumnew = pd.DataFrame()

for file in files:

    data = pd.read_csv(file,  encoding='utf-16')
    data.columns = pd.Series(data.columns).str.replace(' ', '')
    if 'POST' not in data.columns : data['POST'] = 0
    if 'GUPLoss' not in data.columns : data['GUPLoss'] = 0
    print(file)
    datasum = data[['GUPLoss', 'POST']].sum()
    datasum = pd.DataFrame(datasum).transpose()
    datasum['Filename'] = file
    datasum['Number of Rows']=len(data)
    datasumnew = datasumnew.append(datasum)
    #datasumnew = datasumnew.transpose()
    print(datasumnew)

datasumnew.to_csv('c:\data.csv', sep=',', encoding='utf-8',  index=False)

