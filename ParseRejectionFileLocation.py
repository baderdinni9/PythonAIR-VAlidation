#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Parse Location Rejection Validation Script
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


str1 = "_RejectionFileLocation"
TCID = sys.argv[3] #TCID
str4 = "*.txt"
ValID = sys.argv[4]
str5 = TCID + "_Csv_Import_" +  ValID  + str1  + str4
Filename = sys.argv[5]
print(Filename)
os.chdir(Filename)
files = glob.glob(str5)


datacontractnew = pd.DataFrame()
for file in files:
    Filename = file.replace('_'+ ValID,'')
    try:
        data = pd.read_csv(file, encoding='utf-16', sep=',')
    except:
        data = pd.read_csv(file, encoding='utf-8', sep=',')

data.columns = pd.Series(data.columns).str.replace(' ', '')

actualcolumns = [r'ContractID',	r'LocationID',	r'LocationName',	r'ISOBIN',	r'LocationGroup',	r'IsPrimary',	r'IsTenant',	r'InceptionDate',	r'ExpirationDate',	r'Street',	r'City',	r'SubArea2',	r'SubArea',	r'Area',	r'CRESTA',	r'PostalCode',	r'CountryISO',	r'Latitude',	r'Longitude',	r'BuildingValue',	r'OtherValue',	r'ContentsValue',	r'TimeElementValue',	r'DaysCovered',	r'Currency',	r'RiskCount',	r'Premium',	r'ConstructionCodeType',	r'ConstructionCode',	r'ConstructionOther',	r'OccupancyCodeType',	r'OccupancyCode',	r'UDF1',	r'UDF2',	r'UDF3',	r'UDF4',	r'UDF5',	r'AttachedStructures',	r'AdjacentBuildingHeight',	r'BasementLevelCount',	r'BuildingHeight',	r'BuildingHeightUnitCode',	r'CustomFloodSOP',	r'CustomElevation',	r'FirstFloorHeight',	r'FloorOfInterest',	r'FloorArea',	r'FloorAreaUnitCode',	r'FloorsOccupied',	r'GrossArea',	r'GrossAreaUnit',	r'NumberOfStories',	r'ProjectCompletion',	r'RoofYearBuilt',	r'Territory',	r'YearBuilt',	r'FIRMCompliance',	r'Chimney',	r'GlassType',	r'GlassPercent',	r'ExternalDoors',	r'BuildingExteriorOpening',	r'BrickVeneer',	r'FoundationConnection',	r'FoundationType',	r'InternalPartition',	r'LatticeType',	r'IsValueType',	r'ColumnBasement',	r'ColdFormedTube',	r'AppurtenantStructures',	r'LargeMissile',	r'BasementFinishType',	r'BaseFloodElevation',	r'ContentVulnerability',	r'CustomFloodZone',	r'BuildingCondition',	r'BuildingShape',	r'Equipment',	r'MultiStoryHallType',	r'SealofApproval',	r'ServiceEquipmentProtection',	r'Redundancy',	r'RoofGeometry',	r'RoofPitch',	r'RoofCover',	r'RoofDeck',	r'RoofCoverAttachment',	r'RoofDeckAttachment',	r'RoofAnchorage',	r'RoofAttachedStructure',	r'ProjectPhaseCode',	r'Retrofit',	r'SoftStory',	r'ShapeIrregularity',	r'SpecialConstruction',	r'ShortColumn',	r'TallOneStory',	r'Torsion',	r'TransitionInSRC',	r'Tank',	r'TreeExposure',	r'SmallDebris',	r'TerrainRoughness',	r'RoofHailImpactResistance',	r'Ornamentation',	r'Pounding',	r'WaterHeater',	r'WallType',	r'WallSiding',	r'Welding',	r'WindowProtection',	r'LocPerils',	r'LocLimitType',	r'LimitBldg',	r'LimitOther',	r'LimitContent',	r'LimitTime',	r'Participation1',	r'Participation2',	r'DeductType',	r'DeductBldg',	r'DeductOther',	r'DeductContent',	r'DeductTime',	r'SublimitArea',	r'LocPerils 2',	r'LocLimitType 2',	r'LimitBldg 2',	r'LimitOther 2',	r'LimitContent 2',	r'LimitTime 2',	r'Participation1 2',	r'Participation2 2',	r'DeductType 2',	r'DeductBldg 2',	r'DeductOther 2',	r'DeductContent 2',	r'DeductTime 2',	r'SublimitArea 2',	r'LocPerils 3',	r'LocLimitType 3',	r'LimitBldg 3',	r'LimitOther 3',	r'LimitContent 3',	r'LimitTime 3',	r'Participation1 3',	r'Participation2 3',	r'DeductType 3',	r'DeductBldg 3',	r'DeductOther 3',	r'DeductContent 3',	r'DeductTime 3',	r'SublimitArea 3',	r'LocPerils 4',	r'LocLimitType 4',	r'LimitBldg 4',	r'LimitOther 4',	r'LimitContent 4',	r'LimitTime 4',	r'Participation1 4',	r'Participation2 4',	r'DeductType 4',	r'DeductBldg 4',	r'DeductOther 4',	r'DeductContent 4',	r'DeductTime 4',	r'SublimitArea 4',	r'LocPerils 5',	r'LocLimitType 5',	r'LimitBldg 5',	r'LimitOther 5',	r'LimitContent 5',	r'LimitTime 5',	r'Participation1 5',	r'Participation2 5',	r'DeductType 5',	r'DeductBldg 5',	r'DeductOther 5',	r'DeductContent 5',	r'DeductTime 5',	r'SublimitArea 5',	r'LocPerils 6',	r'LocLimitType 6',	r'LimitBldg 6',	r'LimitOther 6',	r'LimitContent 6',	r'LimitTime 6',	r'Participation1 6',	r'Participation2 6',	r'DeductType 6',	r'DeductBldg 6',	r'DeductOther 6',	r'DeductContent 6',	r'DeductTime 6',	r'SublimitArea 6',r'TouchstoneErrorCodes']
columnmissing = set(actualcolumns)-set(data.columns)
print(columnmissing)
for column in columnmissing:
    data[column] = 0

datacontract = data[[r'ContractID',	r'LocationID',	r'LocationName',	r'ISOBIN',	r'LocationGroup',	r'IsPrimary',	r'IsTenant',	r'InceptionDate',	r'ExpirationDate',	r'Street',	r'City',	r'SubArea2',	r'SubArea',	r'Area',	r'CRESTA',	r'PostalCode',	r'CountryISO',	r'Latitude',	r'Longitude',	r'BuildingValue',	r'OtherValue',	r'ContentsValue',	r'TimeElementValue',	r'DaysCovered',	r'Currency',	r'RiskCount',	r'Premium',	r'ConstructionCodeType',	r'ConstructionCode',	r'ConstructionOther',	r'OccupancyCodeType',	r'OccupancyCode',	r'UDF1',	r'UDF2',	r'UDF3',	r'UDF4',	r'UDF5',	r'AttachedStructures',	r'AdjacentBuildingHeight',	r'BasementLevelCount',	r'BuildingHeight',	r'BuildingHeightUnitCode',	r'CustomFloodSOP',	r'CustomElevation',	r'FirstFloorHeight',	r'FloorOfInterest',	r'FloorArea',	r'FloorAreaUnitCode',	r'FloorsOccupied',	r'GrossArea',	r'GrossAreaUnit',	r'NumberOfStories',	r'ProjectCompletion',	r'RoofYearBuilt',	r'Territory',	r'YearBuilt',	r'FIRMCompliance',	r'Chimney',	r'GlassType',	r'GlassPercent',	r'ExternalDoors',	r'BuildingExteriorOpening',	r'BrickVeneer',	r'FoundationConnection',	r'FoundationType',	r'InternalPartition',	r'LatticeType',	r'IsValueType',	r'ColumnBasement',	r'ColdFormedTube',	r'AppurtenantStructures',	r'LargeMissile',	r'BasementFinishType',	r'BaseFloodElevation',	r'ContentVulnerability',	r'CustomFloodZone',	r'BuildingCondition',	r'BuildingShape',	r'Equipment',	r'MultiStoryHallType',	r'SealofApproval',	r'ServiceEquipmentProtection',	r'Redundancy',	r'RoofGeometry',	r'RoofPitch',	r'RoofCover',	r'RoofDeck',	r'RoofCoverAttachment',	r'RoofDeckAttachment',	r'RoofAnchorage',	r'RoofAttachedStructure',	r'ProjectPhaseCode',	r'Retrofit',	r'SoftStory',	r'ShapeIrregularity',	r'SpecialConstruction',	r'ShortColumn',	r'TallOneStory',	r'Torsion',	r'TransitionInSRC',	r'Tank',	r'TreeExposure',	r'SmallDebris',	r'TerrainRoughness',	r'RoofHailImpactResistance',	r'Ornamentation',	r'Pounding',	r'WaterHeater',	r'WallType',	r'WallSiding',	r'Welding',	r'WindowProtection',	r'LocPerils',	r'LocLimitType',	r'LimitBldg',	r'LimitOther',	r'LimitContent',	r'LimitTime',	r'Participation1',	r'Participation2',	r'DeductType',	r'DeductBldg',	r'DeductOther',	r'DeductContent',	r'DeductTime',	r'SublimitArea',	r'LocPerils 2',	r'LocLimitType 2',	r'LimitBldg 2',	r'LimitOther 2',	r'LimitContent 2',	r'LimitTime 2',	r'Participation1 2',	r'Participation2 2',	r'DeductType 2',	r'DeductBldg 2',	r'DeductOther 2',	r'DeductContent 2',	r'DeductTime 2',	r'SublimitArea 2',	r'LocPerils 3',	r'LocLimitType 3',	r'LimitBldg 3',	r'LimitOther 3',	r'LimitContent 3',	r'LimitTime 3',	r'Participation1 3',	r'Participation2 3',	r'DeductType 3',	r'DeductBldg 3',	r'DeductOther 3',	r'DeductContent 3',	r'DeductTime 3',	r'SublimitArea 3',	r'LocPerils 4',	r'LocLimitType 4',	r'LimitBldg 4',	r'LimitOther 4',	r'LimitContent 4',	r'LimitTime 4',	r'Participation1 4',	r'Participation2 4',	r'DeductType 4',	r'DeductBldg 4',	r'DeductOther 4',	r'DeductContent 4',	r'DeductTime 4',	r'SublimitArea 4',	r'LocPerils 5',	r'LocLimitType 5',	r'LimitBldg 5',	r'LimitOther 5',	r'LimitContent 5',	r'LimitTime 5',	r'Participation1 5',	r'Participation2 5',	r'DeductType 5',	r'DeductBldg 5',	r'DeductOther 5',	r'DeductContent 5',	r'DeductTime 5',	r'SublimitArea 5',	r'LocPerils 6',	r'LocLimitType 6',	r'LimitBldg 6',	r'LimitOther 6',	r'LimitContent 6',	r'LimitTime 6',	r'Participation1 6',	r'Participation2 6',	r'DeductType 6',	r'DeductBldg 6',	r'DeductOther 6',	r'DeductContent 6',	r'DeductTime 6',	r'SublimitArea 6',r'TouchstoneErrorCodes']]
datacontract = pd.DataFrame(datacontract)

datacontract['FileName'] = Filename
datacontract = order(datacontract , ['FileName'])
datacontractnew = datacontractnew.append(datacontract)




datacontractnew.to_csv(OUTFILE, sep=',', encoding='utf-8', index=False)