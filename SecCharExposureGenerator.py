
import copy
import pip


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




import pyodbc


sectype = {'ISFA',	'ED',	'RYB',	'RG',	'BV',	'LM',	'ORN',	'WA',	'BFL',	'RM',	'SS',	'BC',	'FFH',	'CHIMNEY',	'TANK',	'WP',	'CFZ',	'RATT',	'All',	'SD',	'RD',	'PC',	'RED',	'SC',	'SI',	'FOI',	'TR',	'FT',	'FC',	'WD',	'AS',	'WS',	'TE',	'BF',	'FIRMC',	'WH',	'IP',	'LT',	'RP',	'BE',	'RA',	'RCA',	'SEP',	'EQP',	'WT',	'RDA',	'SOA',	'TIST',	'FO',	'RC',	'CE',	'CSIBHS',	'POUND',	'TOR',	'RHIR',	'GP',	'CF',	'CBT',	'TOS',	'GT',	'IVT',	'CFT',	'PP',	'BS',	'AB',	'SERS',	'BL',	'CV',	'MSHT'};
server = "qa-ts-153-db1\sql2014"
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server)
cursor = cnxn.cursor()


for sectype in sectype:
    script = 'SELECT * FROM [SecCharDB].[dbo].[locationmapping] where left(LocationID, charindex('+"'"+'_'+"'"+', LocationID) - 1) like ('+"'"+sectype+"'"+') order by left(LocationID, charindex('+"'"+'_'+"'"+', LocationID) - 1),con,occ,age,height,CAST('+"'"+'<x>'+"'"+' + REPLACE(LocationID,'+"'"+"_"+"'"+','+"'"+'</x><x>'+"'"+') + '+"'"+'</x>'+"'"+ 'AS XML).value('+"'"+"/"+'x[2]'+"'"+','+"'"+'int'+"'"+')'
    print(script)

    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server)
    cursor = cnxn.cursor()
    filename = r'\\qafile2\Leonardo\Feature Data\ValidationUnitetest_vasista\SecondaryCharacteristics\InputFiles\Location'+sectype+'.csv'
    with open(filename, 'a') as f:
        DatabaseLossresult = pd.read_sql_query(script, cnxn, chunksize=1000)
        count = 0
        for chunk in DatabaseLossresult:

            header = chunk.columns
            if count == 0:
                chunk.to_csv(f, sep=',', encoding='utf-8', index=False, header = True)
                count += 1
            else:
                chunk.to_csv(f, sep=',', encoding='utf-8', index=False, header = False)


