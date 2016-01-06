#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
EDM Location Validation
******************************************************
"""



__author__ = 'Vasista'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Vasista'
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

    def fetchData(self,EDMDatabase):

        script = 'Select *,ROW_NUMBER() over (partition by t.LOCID order by t.LOCID) RN from (select ' \
                 ' distinct  substring(ACCGRPNAME, 1, 24) +'''' '_' ''''+ CONVERT(VARCHAR(max), Convert(VARCHAR(max), a.ACCGRPID)) ContractID,' \
                 ' substring(ACCGRPNAME, 1, 24) +'''' '_' ''''+CONVERT(VARCHAR(max), Convert(VARCHAR(max),  a.ACCGRPID))+'''' '_' ''''+ CONVERT(VARCHAR(max), Convert(VARCHAR(max),		c.locid)) LocationID,' \
                 ' case when f.EQSLINS=1 then j.UPXPeril +'''' ' + PSL' '''' else UPXPeril end LocationPeril' \
                 '  ,	e.LOCID LOCID,	e.PERIL LOCPERIL,	e.LIMITAMT LOCLIMITAMT,	e.LIMITCUR LOCLIMITCUR,	e.DEDUCTAMT LOCDEDUCTAMT,	e.DEDUCTCUR LOCDEDUCTCUR,	e.VALUEAMT LOCVALUEAMT,	e.VALUECUR LOCVALUECUR,	e.LOSSTYPE LOCLOSSTYPE,	e.LABELID LOCLABELID,	e.COVGMOD LOCCOVGMOD,	e.ACCUMID LOCACCUMID,	e.EQSLGRADE LOCEQSLGRADE,	e.PCTSPRNKLR LOCPCTSPRNKLR,	e.WAITINGPERIOD LOCWAITINGPERIOD,	e.BIPOI LOCBIPOI,	e.ISVALID LOCISVALID,' \
                 '  f.EQDETID EQEQDETID,	f.LOCID EQLOCID,	f.RMSCLASS EQRMSCLASS,	f.ISOCLASS EQISOCLASS,	f.ATCCLASS EQATCCLASS,	f.FIRECLASS EQFIRECLASS,	f.USERCLASS EQUSERCLASS,	f.ATCOCC EQATCOCC,	f.SICOCC EQSICOCC,	f.ISOOCC EQISOOCC,	f.IBCOCC EQIBCOCC,	f.USEROCC EQUSEROCC,	f.YEARUPGRAD EQYEARUPGRAD,	f.PCNTCOMPLT EQPCNTCOMPLT,	f.STARTDATE EQSTARTDATE,	f.COMPDATE EQCOMPDATE,	f.SITELIMAMT EQSITELIMAMT,	f.SITELIMCUR EQSITELIMCUR,	f.SITEDEDAMT EQSITEDEDAMT,	f.SITEDEDCUR EQSITEDEDCUR,	f.COMBINEDLIMAMT EQCOMBINEDLIMAMT,	f.COMBINEDLIMCUR EQCOMBINEDLIMCUR,	f.COMBINEDDEDAMT EQCOMBINEDDEDAMT,	f.COMBINEDDEDCUR EQCOMBINEDDEDCUR,	f.SHAPECONF EQSHAPECONF,	f.STORYPROF EQSTORYPROF,	f.OVERPROF EQOVERPROF,	f.REDUND EQREDUND,	f.TORSION EQTORSION,	f.CLADDING EQCLADDING,	f.BLDGEXT EQBLDGEXT,	f.SHORTCOL EQSHORTCOL,	f.MASINTPART EQMASINTPART,	f.ORNAMENT EQORNAMENT,	f.MECHELEC EQMECHELEC,	f.CONQUAL EQCONQUAL,	f.DURESS EQDURESS,	f.POUNDING EQPOUNDING,	f.TANK EQTANK,	f.HAZEXP EQHAZEXP,	f.ENGFOUND EQENGFOUND,	f.WALLSBRACD EQWALLSBRACD,	f.FRAMEBOLT EQFRAMEBOLT,	f.TILTUPRET EQTILTUPRET,	f.URMPROV EQURMPROV,	f.STRUCTUP EQSTRUCTUP,	f.LANDSLIDE EQLANDSLIDE,	f.LANDCOV EQLANDCOV,	f.LANDMATCH EQLANDMATCH,	f.LIQUEFACT EQLIQUEFACT,	f.LIQUECOV EQLIQUECOV,	f.LIQUEMATCH EQLIQUEMATCH,	f.SOILTYPE EQSOILTYPE,	f.SOILCOV EQSOILCOV,	f.SOILPERIOD EQSOILPERIOD,	f.SOILTHICKNESS EQSOILTHICKNESS,	f.SOILMATCH EQSOILMATCH,	f.BIZONE EQBIZONE,	f.ALQPREZONE EQALQPREZONE,	f.FIRE1 EQFIRE1,	f.FIRE2 EQFIRE2,	f.FIRE3 EQFIRE3,	f.FIRE4 EQFIRE4,	f.FIRE5 EQFIRE5,	f.FIRE6 EQFIRE6,	f.FIRE7 EQFIRE7,	f.FIRE8 EQFIRE8,	f.DUCTILITY EQDUCTILITY,	f.LONGSPAN EQLONGSPAN,	f.BASEISOL EQBASEISOL,	f.SIDINGTYPE EQSIDINGTYPE,	f.PREFAB EQPREFAB,	f.EQSLINS EQEQSLINS,	f.SCNDSUPPLY EQSCNDSUPPLY,	f.EQSLSUSCEPTIBILITY EQEQSLSUSCEPTIBILITY,	f.SPNKLRTYPE EQSPNKLRTYPE,	f.YEARSPNKLR EQYEARSPNKLR,	f.ISVALID EQISVALID,	f.MMI100 EQMMI100,	f.AVGSLOPE EQAVGSLOPE,	f.DISTFAULT1 EQDISTFAULT1,	f.MMI200 EQMMI200,	f.MMI250 EQMMI250,	f.MMI475 EQMMI475,	f.APMATCH EQAPMATCH,	f.MMIMATCH EQMMIMATCH,	f.DISTSINKHOLE EQDISTSINKHOLE,	f.DISTMINE EQDISTMINE,	f.SINKHOLEZONE EQSINKHOLEZONE,' \
                 ' g.HUDETID HUHUDETID,	g.LOCID HULOCID,	g.RMSCLASS HURMSCLASS,	g.FIRECLASS HUFIRECLASS,	g.USERCLASS HUUSERCLASS,	g.ATCOCC HUATCOCC,	g.SICOCC HUSICOCC,	g.ISOOCC HUISOOCC,	g.IBCOCC HUIBCOCC,	g.USEROCC HUUSEROCC,	g.YEARUPGRAD HUYEARUPGRAD,	g.PCNTCOMPLT HUPCNTCOMPLT,	g.STARTDATE HUSTARTDATE,	g.COMPDATE HUCOMPDATE,	g.SITELIMAMT HUSITELIMAMT,	g.SITELIMCUR HUSITELIMCUR,	g.SITEDEDAMT HUSITEDEDAMT,	g.SITEDEDCUR HUSITEDEDCUR,	g.COMBINEDLIMAMT HUCOMBINEDLIMAMT,	g.COMBINEDLIMCUR HUCOMBINEDLIMCUR,	g.COMBINEDDEDAMT HUCOMBINEDDEDAMT,	g.COMBINEDDEDCUR HUCOMBINEDDEDCUR,	g.DESIGNCODE HUDESIGNCODE,	g.CONSTQUALI HUCONSTQUALI,	g.ROOFSYS HUROOFSYS,	g.ROOFGEOM HUROOFGEOM,	g.ROOFANCH HUROOFANCH,	g.ROOFEQUIP HUROOFEQUIP,	g.ROOFAGE HUROOFAGE,	g.ROOFFRAME HUROOFFRAME,	g.ROOFMAINT HUROOFMAINT,	g.ROOFPARAPT HUROOFPARAPT,	g.BASEMENT HUBASEMENT,	g.EXTORN HUEXTORN,	g.CLADSYS HUCLADSYS,	g.CLADRATE HUCLADRATE,	g.FOUNDSYS HUFOUNDSYS,	g.ARCHITECT HUARCHITECT,	g.MECHGROUND HUMECHGROUND,	g.MECHSIDE HUMECHSIDE,	g.WINDMISSL HUWINDMISSL,	g.FLOODMISSL HUFLOODMISSL,	g.FLOODPROT HUFLOODPROT,	g.VULNWIND HUVULNWIND,	g.VULNFLOOD HUVULNFLOOD,	g.MANROUGH HUMANROUGH,	g.MMRCOV HUMMRCOV,	g.NATROUGH HUNATROUGH,	g.NATCOV HUNATCOV,	g.EXPOSURE HUEXPOSURE,	g.FLOODINDEX HUFLOODINDEX,	g.ELEVATION HUELEVATION,	g.ELEVCOV HUELEVCOV,	g.DISTCOAST HUDISTCOAST,	g.SYSDISTCST HUSYSDISTCST,	g.DISTCSTCOV HUDISTCSTCOV,	g.TOPOFEAT HUTOPOFEAT,	g.WINDPOOL HUWINDPOOL,	g.COASTSEG HUCOASTSEG,	g.RESISTDOOR HURESISTDOOR,	g.RESISTOPEN HURESISTOPEN,	g.ELEVMATCH HUELEVMATCH,	g.NRUFMATCH HUNRUFMATCH,	g.MMRUFMATCH HUMMRUFMATCH,	g.DSCSTMATCH HUDSCSTMATCH,	g.WPOOLMATCH HUWPOOLMATCH,	g.ISVALID HUISVALID,	g.NFIP_RATE HUNFIP_RATE,	g.BUILDINGELEVATION HUBUILDINGELEVATION,	g.BUILDINGELEVATIONMATCH HUBUILDINGELEVATIONMATCH,	g.NFIPYEAR HUNFIPYEAR,	g.NFIPYEARMATCH HUNFIPYEARMATCH,	g.FLASHING HUFLASHING,	g.BIZONE HUBIZONE,	g.RMSBUILDINGELEVATION HURMSBUILDINGELEVATION,	g.RMSBUILDINGELEVATIONMATCH HURMSBUILDINGELEVATIONMATCH,	g.IFMVerticalExpDist HUIFMVerticalExpDist,	g.IFMStructCondition HUIFMStructCondition,	g.IFMEquipBracing HUIFMEquipBracing,	g.IFMMissileExp HUIFMMissileExp,' \
                 ' i.FLDETID FLFLDETID,	i.LOCID FLLOCID,	i.FLZONE FLFLZONE,	i.BFE FLBFE,	i.ADDLINFO FLADDLINFO,	i.PANEL FLPANEL,	i.COBRA FLCOBRA,	i.FLOODWAY FLFLOODWAY,	i.SFHA FLSFHA,	i.COMMUNITY FLCOMMUNITY,	i.CONFIDENCE FLCONFIDENCE,	i.UNDERREV FLUNDERREV,	i.ISVALID FLISVALID,	i.SITELIMAMT FLSITELIMAMT,	i.SITELIMCUR FLSITELIMCUR,	i.SITEDEDAMT FLSITEDEDAMT,	i.SITEDEDCUR FLSITEDEDCUR,	i.COMBINEDLIMAMT FLCOMBINEDLIMAMT,	i.COMBINEDLIMCUR FLCOMBINEDLIMCUR,	i.COMBINEDDEDAMT FLCOMBINEDDEDAMT,	i.COMBINEDDEDCUR FLCOMBINEDDEDCUR,	i.YEARUPGRAD FLYEARUPGRAD,	i.PCNTCOMPLT FLPCNTCOMPLT,	i.STARTDATE FLSTARTDATE,	i.COMPDATE FLCOMPDATE,	i.BUFFER FLBUFFER,	i.PANELDATE FLPANELDATE,	i.OTHERZONES FLOTHERZONES,	i.FLMATCH FLFLMATCH,	i.FINISHEDFLOOR FLFINISHEDFLOOR,	i.MAPSOURCE FLMAPSOURCE,	i.USERID1 FLUSERID1,	i.USERID2 FLUSERID2,	i.FLOODPROT FLFLOODPROT,	i.FLOORTYPE FLFLOORTYPE,	i.ELEVATION FLELEVATION,	i.FLOODINDEX FLFLOODINDEX,	i.ELEVCOV FLELEVCOV,	i.FLOODMISSL FLFLOODMISSL,	i.BASEMENT FLBASEMENT,	i.VULNFLOOD FLVULNFLOOD,	i.MECHGROUND FLMECHGROUND,	i.CLADSYS FLCLADSYS,	i.FOUNDSYS FLFOUNDSYS,	i.ANNPROB FLANNPROB,																																																																																																																																																																	h.TODETID TOTODETID,	h.LOCID TOLOCID,	h.RMSCLASS TORMSCLASS,	h.FIRECLASS TOFIRECLASS,	h.USERCLASS TOUSERCLASS,	h.ATCOCC TOATCOCC,	h.ISOOCC TOISOOCC,	h.USEROCC TOUSEROCC,	h.YEARUPGRAD TOYEARUPGRAD,	h.PCNTCOMPLT TOPCNTCOMPLT,	h.SITELIMAMT TOSITELIMAMT,	h.SITELIMCUR TOSITELIMCUR,	h.SITEDEDAMT TOSITEDEDAMT,	h.SITEDEDCUR TOSITEDEDCUR,	h.COMBINEDLIMAMT TOCOMBINEDLIMAMT,	h.COMBINEDLIMCUR TOCOMBINEDLIMCUR,	h.COMBINEDDEDAMT TOCOMBINEDDEDAMT,	h.COMBINEDDEDCUR TOCOMBINEDDEDCUR,	h.DESIGNCODE TODESIGNCODE,	h.CONSTQUALI TOCONSTQUALI,	h.ROOFSYS TOROOFSYS,	h.ROOFGEOM TOROOFGEOM,	h.ROOFANCH TOROOFANCH,	h.ROOFAGE TOROOFAGE,	h.ROOFFRAME TOROOFFRAME,	h.ROOFMAINT TOROOFMAINT,	h.ROOFPARAPT TOROOFPARAPT,	h.ROOFEQUIP TOROOFEQUIP,	h.EXTORN TOEXTORN,	h.CLADSYS TOCLADSYS,	h.CLADRATE TOCLADRATE,	h.FOUNDSYS TOFOUNDSYS,	h.ARCHITECT TOARCHITECT,	h.MECHSIDE TOMECHSIDE,	h.WINDMISSL TOWINDMISSL,	h.VULNWIND TOVULNWIND,	h.VULNFLOOD TOVULNFLOOD,	h.RESISTDOOR TORESISTDOOR,	h.RESISTOPEN TORESISTOPEN,	h.ISVALID TOISVALID,	h.BASEMENT TOBASEMENT,	h.MECHGROUND TOMECHGROUND,	h.FLOODMISSL TOFLOODMISSL,	h.FLOODPROT TOFLOODPROT,	h.ICEPROTECT TOICEPROTECT,	h.PLUMBING TOPLUMBING,	h.ATTICINSUL TOATTICINSUL,	h.ROOFVENT TOROOFVENT,	h.SNOWGUARDS TOSNOWGUARDS,	h.TREEDENSITY TOTREEDENSITY,	h.GARAGING TOGARAGING' \
                 ' from '+EDMDatabase+'..accgrp a' \
                 ' full join '+EDMDatabase+'..policy b on a.ACCGRPID=b.ACCGRPID' \
                                           ' full join '+EDMDatabase+'..Property c on c.ACCGRPID=a.ACCGRPID' \
                                                                     ' full join '+EDMDatabase+'..Address d on d.AddressID=c.AddressID' \
                                                                                               ' full join '+EDMDatabase+'..loccvg e on e.LOCID=c.LOCID' \
                                                                                                                         ' full join '+EDMDatabase+'..eqdet f on f.LOCID=e.LOCID' \
                                                                                                                                                   ' full join '+EDMDatabase+'..hudet g on g.LOCID=e.LOCID' \
                                                                                                                                                                             ' full join '+EDMDatabase+'..todet h on h.LOCID=e.LOCID' \
                                                                                                                                                                                                       ' full join '+EDMDatabase+'..fldet i on i.LOCID=e.LOCID' \
                                                                                                                                                                                                                                 ' left join AIRReference..tConvertEDCPeril j on j.EDCPerilcode=e.PERIL where e.locid is not NULL  ) t order by t.locid'

        return copy.deepcopy(pd.read_sql(script, self.connection))




server = sys.argv[3]
EDMDatabase = sys.argv[4]
db = Database(server)
EDMLocation = db.fetchData(EDMDatabase)

#print(EDMLocation)


uniqueID = pd.unique(EDMLocation.columns)
#print(uniqueID)



tempEDMLocationtoappend =  pd.DataFrame()

for colnames in uniqueID:

    tempEDMLocation = EDMLocation.pivot(index = 'LOCID', columns='RN',values = colnames)
    tempEDMLocation.columns = colnames +  pd.Series(tempEDMLocation.columns).astype(str)
    tempEDMLocation['LOCID'] = tempEDMLocation.index
    tempEDMLocationtoappend['LOCID'] = tempEDMLocation.index
    #print(tempEDMLocation)
    tempEDMLocationtoappend = pd.merge(tempEDMLocationtoappend, tempEDMLocation, on = 'LOCID')

print(tempEDMLocationtoappend)
#tempEDMLocationtoappend.to_csv('c:\data2.csv', sep=',', encoding='utf-8', index = False)

tempEDMLocationtoappend.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)

