########################################################################

# Title: Bioregions Automation

# Authors: Matear, L.(2019),
# Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:    Read in all data from Bioregions extract and
# MarLIN-MarESA Biotopes extract Resilience data to be aggregated
# between EUNIS levels. This task allows the user to explore the
# connectivity of data across varying resolutions of detail.
#
# To ensure no permanent alterations are made to the master documents,
# all data used within this script are copies of the original files.
#
# For any enquiries please contact Liam Matear by email:
# Liam.Matear@jncc.gov.uk

########################################################################

#   Introduction & Setup

########################################################################

# Section 1: Introduction
# For visual overview, see 'Methodology Infographic' file within
# the 'Bioregions Automation' folder

# Process Outline:

# Import all libraries required for code execution within the ESRI
# ArcGIS Geoprocessing Python Console only
# import arcpy

# Import all libraries required for code execution within the IDE
# Import libraries for data manipulation
import os
import re
import time
import numpy as np
import pandas as pd
import json

# Import libraries for text mining. nltk important for text editting
import nltk
# Need to download the first time
#nltk.download('punkt')
#nltk.download('averaged_perceptron_tagger')

# These are all imports for the text mining part
# import slate3k as slate
# from os import path
# from PIL import Image # Pillow
# from nltk.corpus import stopwords
# from nltk.stem.porter import PorterStemmer
# from nltk.tokenize import RegexpTokenizer
# from nltk.stem.wordnet import WordNetLemmatizer
# from sklearn.feature_extraction.text import CountVectorizer

pd.options.mode.chained_assignment = None  # default='warn'


########################################################################

#   1. Geospatial Manipulations: Marine Recorder Points

########################################################################

##########################
# [THIS SECTION IS WRITTEN IN ARCPY AND CAN ONLY BE EXECUTED FROM ESRI
# ArcGIS PYTHON CONSOLE]
##########################

# 1.1. Adding XY Data
# Load in Marine Recorder shapefile - obtained from the JNCC Marine
# Habitat Mapping Team.

# 1.2. Performing Intersections: MR points into polygons of interest

# 1.2.1. Intersect points data with UK waters inshore / offshore polygon
# data and assign attributes to points - NOT NEEDED?
#arcpy.Intersect_analysis("'MR_Samples Events' #;UK_waters_dissolved #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_UKwaters","ALL","#","INPUT")
#arcpy.Intersect_analysis("'MR_SnapshotC20190726' #;UK_waters_dissolved #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_UKwatersC05082019","ALL","#","INPUT")

# 1.2.2. Intersect outputs of stage X with the Charting Progress 2
# (CP2) regions polygons and assign relevant attributes to points for
# analyses
#arcpy.Intersect_analysis("MR_Samples_Intersect_UKwaters #;P20090401_RegionalSeas_CP2Reporting_WGS84 #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_UKwaters_CP2reg","ALL","#","INPUT")
#arcpy.Intersect_analysis("MR_Samples_Intersect_UKwatersC05082019 #;P20090401_RegionalSeas_CP2Reporting_WGS84 #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_UKwaters_CP2regC05082019","ALL","#","INPUT")

# 1.2.3. Intersect the outputs of stage x with CP2 sub-regions
#arcpy.Intersect_analysis("MR_Samples_Intersect_UKwaters_CP2reg #;CP2_Subregions #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_UKwaters_CP2reg_CP2sub","ALL","#","INPUT")
#arcpy.Intersect_analysis("MR_Samples_Intersect_UKwaters_CP2regC05082019 #;CP2_Subregions #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_UKwaters_CP2reg_CP2subC05082019","ALL","#","INPUT")

# Intersect all MR spatial points data with the bioregions layer
# (corrected version)
#Public MR Version
#arcpy.Intersect_analysis("Bioregions_TOPO #;C20200415_HabitatExtracts_SnapshotDatav51_Public_20200205 #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_BioregionsPublicC20200428","ALL","#","INPUT")
#All MR Version
#arcpy.Intersect_analysis("Bioregions_TOPO #;C2020-09-18_HabitatExtracts_SnapshotDatav51_ALL_20200730 #", "Z:/Marine/Evidence/PressuresImpacts/6. Sensitivity/SA's Contracts/C16-0257-105 Biogeographical Regional Contract/GIS/working_gdb.gdb/MR_Samples_Intersect_BioregionsAllC20201104","ALL","#","INPUT")

########################################################################

#  2. Importing Data To An Integrated Development Environment (IDE)

########################################################################
# Test the run time of the function
start = time.process_time()

# 2.1. Manipulating Data

##########################
# [THIS SECTION IS WRITTEN IN PYTHON AND SHOULD BE EXECUTED FROM A
# CONSOLE / IDE]
##########################

# Define folder file path to be saved into
outpath = "./Bioregions/Output/"

# 2.1.1. Importing intersection attributes as Pandas DataFrames (DF)

# Import MR samples within Bioregions - MR PUBLIC DATA - THIS
# NEEDS UPDATING EACH TIME NEW MR DATA ARE AVAILABLE
MR_Samples = pd.read_csv("./Bioregions/Data/MR_All_BioregionsIntersect_c20220310.csv")
# made from arcpy

# Import all data within the presence absence dataset. Need to duplicate
# Regions 2 and 3 to not lose headers on import - updated from
#'29042020' version of the presence absence spreadsheet on 05/11/2020
presence_absence = pd.read_excel(
    "./Bioregions/Data/Presence absence spreadsheet_Final_JNCC_14072020.xlsx",
    'Biotope_presence_absence', header=1
    )

# Import template automation evidence spreadsheet as provided by E.Last
auto_evidence = pd.read_excel(
    "./Bioregions/Data/Automation_EvidenceFields.xlsx", 'Sheet1')

# Import copy of biotopes database
Biotopes_DB = pd.read_excel("./Bioregions/Data/Biotope database_Final_29042020.xlsx",
                            'Biotope Database')

# Import UKSeaMap2018 data attributes
UKSM = pd.read_csv("./Bioregions/Data/UKSM18_BioregionsIntersection_Attributes.csv")

#         Import NBN species spatial data
NBN_SpeciesSpatial = pd.read_excel(
    "./Bioregions/Data/NBN_Corrected_Data_Merged_Intersected.xlsx",
    'NBN_Corrected_Data_Merged_Inter'
    )

# Import NBN species biotope list data
NBN_SpeciesList = pd.read_excel("./Bioregions/Data/NBN_Species.xlsx", "NBN_Species")

# Import all MPAs within each bioregion
#MPA_Bioregion = pd.read_excel(
#    "./Bioregions/Data/Bioregions_MPAsMergeIntersection_Attributes.xlsx",
#    'Bioregions_MPAsMergeIntersectio'
#    )
# from text mining bit. not there anymore, ask laura if we need it


########################################################################

#      3. Formatting MR Points Data EUNIS ASSIGNED DATA ONLY

########################################################################

# 3.1. Formatting MR Points Attributes to facilitate integration into
# the auto_evidence DF

##########################
# [THIS SECTION IS WRITTEN IN PYTHON AND SHOULD BE EXECUTED FROM A
# CONSOLE / IDE]
##########################

# 3.1.1. Refine the MR_Samples DF to only retain the columns of interest
MR_Samples_Slice = MR_Samples[['EUNIS2007', 'Biotope', 'Region', 'Region_ID']]

#        Rename columns within MR_Samples_Slice to match those within the auto_evidence_DF
MR_Samples_Slice.columns = ['EUNIS code', 'JNCC biotope code', 'Bioregion', 'Region_ID']

#        Replace all empty string values within the EUNIS column of the MR_Samples_Slice DF as NaN
#        This allows the user to remove these using the .dropna() method for the EUNIS only method of data formatting
MR_Samples_Slice['EUNIS code'].replace('', np.nan, inplace=True)
MR_Samples_Slice['EUNIS code'].replace(' ', np.nan, inplace=True)

#        Drop all NaN values within the MR_Samples_Slice 'EUNIS code' Column
MR_Samples_Slice.dropna(subset=['EUNIS code'], inplace=True)

#        Add 'Present in MR' column and set all values to Yes
MR_Samples_Slice['Present in MR'] = 'Yes'

# 3.1.2. Add in a blank set of all other EUNIS biotopes from the presence absence spreadsheet which are not included
#        within the MR samples
Presence_Absence_Slice = presence_absence[['EUNIS code 2007', 'JNCC 15.03 code']]

#        Rename columns within the Presence_Absence_Slice DF to match those of the MR_Samples_Slice
Presence_Absence_Slice.columns = ['EUNIS code', 'JNCC biotope code']

#        Add 'Present in MR' column and set all values to No
Presence_Absence_Slice['Present in MR'] = 'No'

#        Subset the Presence_Absence_Slice to only retain EUNIS codes which are not already contained within the MR data
MR_EUNIS = list(MR_Samples_Slice['EUNIS code'].unique())

#        Use MR_EUNIS list to select all Presence_Absence_Slice data where EUNIS codes do not match
Presence_Absence_Insert = Presence_Absence_Slice.loc[~Presence_Absence_Slice['EUNIS code'].isin(MR_EUNIS)]


#        Append the Presence_Absence_Insert DF into the MR_Samples_Slice DF and assign all blank values a given
#        Bioregion
def data_insertion(target_df, bioregion, region_id):
    """
    Define function tto add in a complete set of EUNIS codes from the presence absence data which are not contained
    within the MR data and complete the relevant Bioregion / ID columns.
    :param target_df: Insert dataset
    :param bioregion: Bioregion of interest
    :param region_id: Bioregion ID of interest
    :return: Updated DF
    """
    # Append all Presence_Absence_Insert data into the target DF
    data = target_df.append(Presence_Absence_Insert, sort=False)
    # Fill all empty entries within the data['Bioregion'] column with bioregion data
    data['Bioregion'].fillna(bioregion, inplace=True)
    # Fill all empty entries within the data['Region_ID'] column with bioregion numerical ID data
    data['Region_ID'].fillna(region_id, inplace=True)
    return data


#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 1a'
Updated_1a = data_insertion(MR_Samples_Slice, 'Sub-region 1a', '1a')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 1b'
Updated_1ab = data_insertion(Updated_1a, 'Sub-region 1b', '1b')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Region 2: Southern North Sea'
Updated_1ab_2 = data_insertion(Updated_1ab, 'Region 2: Southern North Sea', '2')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Region 3: Eastern Channel'
Updated_1ab_2_3 = data_insertion(Updated_1ab_2, 'Region 3: Eastern Channel', '3')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 4a'
Updated_1ab_2_3_4a = data_insertion(Updated_1ab_2_3, 'Sub-region 4a', '4a')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 4b'
Updated_1ab_2_3_4ab = data_insertion(Updated_1ab_2_3_4a, 'Sub-region 4b', '4b')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 5a'
Updated_1ab_2_3_4ab_5a = data_insertion(Updated_1ab_2_3_4ab, 'Sub-region 5a', '5a')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 5b'
Updated_1ab_2_3_4ab_5ab = data_insertion(Updated_1ab_2_3_4ab_5a, 'Sub-region 5b', '5b')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 6a'
Updated_1ab_2_3_4ab_5ab_6a = data_insertion(Updated_1ab_2_3_4ab_5ab, 'Sub-region 6a', '6a')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 7a'
Updated_1ab_2_3_4ab_5ab_6a_7a = data_insertion(Updated_1ab_2_3_4ab_5ab_6a, 'Sub-region 7a', '7a')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Sub-region 7b'
Updated_1ab_2_3_4ab_5ab_6a_7ab = data_insertion(Updated_1ab_2_3_4ab_5ab_6a_7a, 'Sub-region 7b (deep-sea)', '7b')

#         Run the data_insertion() function to insert presence / absence EUNIS data for 'Region 8 (deep-sea)'
Updated_Habitats = data_insertion(Updated_1ab_2_3_4ab_5ab_6a_7ab, 'Region 8 (deep-sea)', '8')

# 3.1.3. Perform merge to combine the MP CP2 Samples into the auto_evidence DF
MR_evidence_merge = pd.merge(Updated_Habitats, auto_evidence, on=['EUNIS code', 'JNCC biotope code', 'Bioregion'],
                             how='outer')

#        Set Present in MR column to the values of the Present in MR_x column and drop the unwanted left only column
MR_evidence_merge['Present in MR_y'] = MR_evidence_merge['Present in MR_x']

#        Drop the unwanted left only 'Present in MR_x' column
MR_evidence_merge.drop(['Present in MR_x'], axis=1, inplace=True)

#        Rename the remaining Present in MR_y column
MR_evidence_merge.rename(columns={'Present in MR_y': 'Present in MR'}, inplace=True)

########################################################################

#                 4. Assigning necessary metadata

########################################################################

# 4.1. Perform string matches to acquire all relevant data from the
# Biotopes Database and insert into the MR_evidence_merge DF.

##########################
# [THIS SECTION IS WRITTEN IN PYTHON AND SHOULD BE EXECUTED FROM A
# CONSOLE / IDE]
##########################

# 4.1.1. Create subset of the Biotopes_DB DF which only retains the
# columns of interest to be added into the MR_evidence_merge DF.
Biotopes_DB_Slice = Biotopes_DB[[
    'EUNIS', 'JNCC', 'Salinity', 'Wave exposure', 'Tidal streams',
    'Substratum', 'Zone', 'Depth band', 'Other features',
    'Description (JNCC, 2015)', 'Characteristic species', 'Climate',
    'Similar biotopes', 'Link to other biotopes', 'References',
    'Comments']]

#        Rename columns within Biotopes_DB_Slice to match those within the MR_evidence_merge_DF
Biotopes_DB_Slice.columns = [
    'EUNIS code', 'JNCC biotope code', 'Salinity', 'Wave exposure',
    'Tidal streams', 'Substratum', 'Zone', 'Depth band',
    'Other features', 'Description (JNCC, 2015)',
    'Characteristic species', 'Climate', 'Similar biotopes',
    'Link to other biotopes', 'References', 'Comments']

# 4.1.2. Perform merge between the Biotopes_DB_Slice and the
# MR_evidence_merge DF based on matching EUNIS and JNCC biotope codes
MR_BiotopesDB_Merge = pd.merge(Biotopes_DB_Slice, MR_evidence_merge,
    on=['EUNIS code', 'JNCC biotope code'], how='outer')

# CHECK MR_BiotopesDB_Merge.loc[MR_BiotopesDB_Merge['EUNIS_code'].isin(['A4.27'])]

# 4.2. Formatting the newly inserted data within the MR_BiotopesDB_Merge DF

# 4.2.1. Removing the unwanted Y / right only duplicate columns from the original auto_evidence DF
MR_BiotopesDB_Merge.drop([
    'Salinity_y', 'Wave exposure_y', 'Tidal streams_y', 'Substratum_y',
    'Zone_y', 'Depth band_y', 'Other features_y',
    'Description (JNCC, 2015)_y', 'Characteristic species_y',
    'Climate_y', 'Similar biotopes_y', 'Link to other biotopes_y',
    'References_y', 'Comments_y'
    ], axis=1, inplace=True)

# 4.2.2. Rename the remaining columns left within the MR_BiotopesDB_Merge DF
MR_BiotopesDB_Merge.columns = [
    'EUNIS_code', 'JNCC biotope code', 'Salinity', 'Wave exposure',
    'Tidal streams', 'Substratum', 'Zone', 'Depth band',
    'Other features', 'Description (JNCC, 2015)',
    'Characteristic species', 'Climate', 'Similar biotopes',
    'Link to other biotopes', 'References', 'Comments', 'Bioregion',
    'Region_ID', 'Present in MR', 'If present, how many records?',
    'Predicted in UK SeaMap?', 'Characterising species in NBN?',
    'L4 parent present (based on data)?', 'Child L5/L6 present?',
    'Similar sibling biotopes present?',
    'Habitat present in literature/survey reports?',
    'Characterising species present in literature/survey reports?',
    'Habitat suitable?', 'Within recorded biotope distribution?',
    'Expert judgement indicates presence?']

# 4.2.3. Counting all entries of a given EUNIS code within a specific location / Bioregion
#        Subset data to only include those which score yes on the present within MR column
CountSubset = MR_BiotopesDB_Merge.loc[MR_BiotopesDB_Merge['Present in MR'].isin(['Yes'])]

#        Preform data aggregation and count the occurrences of EUNIS values within a given EUNIS code per Bioregion
EUNIS_Counts = CountSubset.groupby(['Bioregion', 'EUNIS_code']).EUNIS_code.agg(['count'])

#        Convert data back into Pandas DF format for further data manipulation
EUNIS_Counts = pd.DataFrame(EUNIS_Counts)

#        Reset index of newly created DF to facilitate column formatting
EUNIS_Counts = EUNIS_Counts.reset_index(inplace=False)

#        Add 'Present in MR' column and set all values to Yes
EUNIS_Counts['Present in MR'] = 'Yes'

#        Merge EUNIS_Counts DF back into the MR_BiotopesDB DF where EUNIS_code and Bioregion match
MR_BiotopesDB_Merge = pd.merge(EUNIS_Counts, MR_BiotopesDB_Merge, on=['EUNIS_code', 'Bioregion', 'Present in MR'],
                               how='outer')

#        Replace NaN values within the 'count' column
MR_BiotopesDB_Merge['count'] = MR_BiotopesDB_Merge['count'].fillna(0).astype(int)

#        Convert data within the 'count' column back into an integer
MR_BiotopesDB_Merge['count'] = MR_BiotopesDB_Merge['count'].astype(int)

#        Assign values from the 'count' column to the 'If present, how many records?' column
MR_BiotopesDB_Merge['If present, how many records?'] = MR_BiotopesDB_Merge['count']

#        Drop unwanted 'count' column from DF after copying data to other column
MR_BiotopesDB_Merge.drop(['count'], axis=1, inplace=True)

#        Remove duplicates of EUNIS values within a given Bioregion
MR_BiotopesDB_Merge = MR_BiotopesDB_Merge.drop_duplicates(['Bioregion', 'EUNIS_code'])

#        Fill blank values with 'Not Applicable' within 'If present, how many records?' column
MR_BiotopesDB_Merge['If present, how many records?'] = MR_BiotopesDB_Merge['If present, how many records?'].fillna(0).astype(int)

# 4.3. Completing metadata records

# 4.3.1. Checking EUNIS biotopes against presence within UK SeaMap 2018 (within the bioregion of interest)

# Create subset of all UKSM EUNIS codes per Bioregion
UKSM_R1a = UKSM.loc[UKSM['Region'] == 'Sub-region 1a']
UKSM_R1b = UKSM.loc[UKSM['Region'] == 'Sub-region 1b']
UKSM_R2 = UKSM.loc[UKSM['Region'] == 'Region 2: Southern North Sea']
UKSM_R3 = UKSM.loc[UKSM['Region'] == 'Region 3: Eastern Channel']
UKSM_R4a = UKSM.loc[UKSM['Region'] == 'Sub-region 4a']
UKSM_R4b = UKSM.loc[UKSM['Region'] == 'Sub-region 4b']
UKSM_R5a = UKSM.loc[UKSM['Region'] == 'Sub-region 5a']
UKSM_R5b = UKSM.loc[UKSM['Region'] == 'Sub-region 5b']
UKSM_R6a = UKSM.loc[UKSM['Region'] == 'Sub-region 6a']
UKSM_R7a = UKSM.loc[UKSM['Region'] == 'Sub-region 7a']
UKSM_R7b = UKSM.loc[UKSM['Region'] == 'Sub-region 7b (deep-sea)']
UKSM_R8 = UKSM.loc[UKSM['Region'] ==
                   'Region 8: Atlantic North-West Approaches, Rockall Trough and Faeroe/Shetland Channel']


# Create function which checks the UKSM18 DF for biotopes of interest within
def uksm_check(row):
    # Pull out EUNIS data from the DF
    e_code = row['EUNIS_code']
    # Check if the row entry is correlates to the Bioregion: Sub-region 1a
    if 'Sub-region 1a' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R1a['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 1b
    if 'Sub-region 1b' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R1b['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Region 2: Southern North Sea
    if 'Region 2: Southern North Sea' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R2['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Region 3: Eastern Channel
    if 'Region 3: Eastern Channel' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R3['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 4a
    if 'Sub-region 4a' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R4a['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 4b
    if 'Sub-region 4b' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R4b['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 5a
    if 'Sub-region 5a' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R5a['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 5b
    if 'Sub-region 5b' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R5b['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 6a
    if 'Sub-region 6a' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R6a['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 7a
    if 'Sub-region 7a' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R7a['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Sub-region 7b (deep-sea)
    if 'Sub-region 7b (deep-sea)' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R7b['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'
    # Check if the row entry is correlates to the Bioregion: Region 8 (deep-sea)
    if 'Region 8 (deep-sea)' in str(row['Bioregion']):
        # Check if the EUNIS code being analysed exists within the unique EUNIS codes within the given location
        if len(str(e_code)) in [x for x in list(UKSM_R8['EUNIScomb'].unique())]:
            # If found, return 'Present'
            return 'Present'
        else:
            # If not found, return 'Absent'
            return 'Absent'


# Utilise the uksm_check() function to analyse the EUNIS codes, and establish if the same EUNIS code has been recorded
# within UK SeaMap 2018 (within the relevant Bioregion of interest). This function is applied to the entire DataFrame
# using the .apply() method and lambda calculus.
MR_BiotopesDB_Merge['Predicted in UK SeaMap?'] = MR_BiotopesDB_Merge.apply(lambda row: uksm_check(row), axis=1)



# UKSM_EUNIS = list(UKSM['EUNIScomb'].unique())
#
# #        Assign all entries where the MR_BiotopesDB_Merge EUNIS_code value is present within the UKSM_EUNIS list the
# #        value 'Present'
# MR_BiotopesDB_Merge.loc[MR_BiotopesDB_Merge['EUNIS_code'].isin(UKSM_EUNIS), 'Predicted in UK SeaMap?'] = 'Present'
#
# #        Assign all entries where the MR_BiotopesDB_Merge EUNIS_code value is not present within the UKSM_EUNIS list the
# #        value 'Absent'
# MR_BiotopesDB_Merge.loc[~MR_BiotopesDB_Merge['EUNIS_code'].isin(UKSM_EUNIS), 'Predicted in UK SeaMap?'] = 'Absent'

# 4.3.2. Checking the presence / absence of NBN characterising species in each entry of the DF

#        Create object oriented variables for all species present from the NBN records within each bioregion and set
#        as a list
#        Region 1a
Species_R1a = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 1a')]
Species_R1a = list(Species_R1a['taxonname'])
#        Region 1b
Species_R1b = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 1b')]
Species_R1b = list(Species_R1b['taxonname'])
#        Region 2
Species_R2 = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Region 2: Southern North Sea')]
Species_R2 = list(Species_R2['taxonname'])
#        Region 3
Species_R3 = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Region 3: Eastern Channel')]
Species_R3 = list(Species_R3['taxonname'])
#        Region 4a
Species_R4a = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 4a')]
Species_R4a = list(Species_R4a['taxonname'])
#        Region 4b
Species_R4b = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 4b')]
Species_R4b = list(Species_R4b['taxonname'])
#        Region 5a
Species_R5a = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 5a')]
Species_R5a = list(Species_R5a['taxonname'])
#        Region 5b
Species_R5b = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 5b')]
Species_R5b = list(Species_R5b['taxonname'])
#        Region 6a
Species_R6a = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 6a')]
Species_R6a = list(Species_R6a['taxonname'])
#        Region 7a
Species_R7a = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 7a')]
Species_R7a = list(Species_R7a['taxonname'])
#        Region 7b
Species_R7b = NBN_SpeciesSpatial.loc[(NBN_SpeciesSpatial['Region'] == 'Sub-region 7b')]
Species_R7b = list(Species_R7b['taxonname'])
#        Region 7b
Species_R8 = NBN_SpeciesSpatial.loc[
    (NBN_SpeciesSpatial['Region'] ==
     'Region 8: Atlantic North-West Approaches, Rockall Trough and Faeroe/Shetland Channel')]
Species_R8 = list(Species_R8['taxonname'])

#        Perform merge between the MR_BiotopesDB_Merge DF and the NBN Df
MR_BiotopesDB_Merge = pd.merge(MR_BiotopesDB_Merge, NBN_SpeciesList, left_on='EUNIS_code', right_on='EUNIS Biotopes',
                               how='outer')

#        Assign 'Yes' / 'No' to all  entries where the 'Species' column either contains or does not contain data
MR_BiotopesDB_Merge.loc[MR_BiotopesDB_Merge['Species'].isna(), 'Characterising species in NBN?'] = 'Not Applicable'
MR_BiotopesDB_Merge.loc[MR_BiotopesDB_Merge['Species'].notna(), 'Characterising species in NBN?'] = 'Yes'

# Create function to check if value for 'Characterising species
# in NBN? == 'Yes', if True, check if the
# correlating species listed within the NBN species list is
# present in the relevant NBN species spatial data.
# If this is also True, then return the string value 'Yes'


def nbn_check(df):
    # Pull out presence in NBN species list value for DF
    presence = df['Characterising species in NBN?']
    # Pull out relevant species listed in the species column
    species = str(df['Species'])
    # Pull out the relevant bioregion entry
    bioregion = str(df['Bioregion'])
    # Perform first test if the presence value is recorded as 'Yes' or 'Not Applicable'
    if presence == 'Yes':
        # Check if the bioregion is 1a
        if bioregion == 'Sub-region 1a':
            # Check if the relevant species entry is present in the Species_R1a list
            if str(species) in Species_R1a:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 1b
        elif bioregion == 'Sub-region 1b':
            # Check if the relevant species entry is present in the Species_R1b list
            if str(species) in Species_R1b:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 2
        elif bioregion == 'Region 2: Southern North Sea':
            # Check if the relevant species entry is present in the Species_R2 list
            if str(species) in Species_R2:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 3
        elif bioregion == 'Region 3: Eastern Channel':
            # Check if the relevant species entry is present in the Species_R3 list
            if str(species) in Species_R3:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 4a
        elif bioregion == 'Sub-region 4a':
            # Check if the relevant species entry is present in the Species_R4a list
            if str(species) in Species_R4a:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 4b
        elif bioregion == 'Sub-region 4b':
            # Check if the relevant species entry is present in the Species_R4b list
            if str(species) in Species_R4b:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 5a
        elif bioregion == 'Sub-region 5a':
            # Check if the relevant species entry is present in the Species_R5a list
            if str(species) in Species_R5a:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 5b
        elif bioregion == 'Sub-region 5b':
            # Check if the relevant species entry is present in the Species_R5b list
            if str(species) in Species_R5b:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 6a
        elif bioregion == 'Sub-region 6a':
            # Check if the relevant species entry is present in the Species_R6a list
            if str(species) in Species_R6a:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 7a
        elif bioregion == 'Sub-region 7a':
            # Check if the relevant species entry is present in the Species_R7a list
            if str(species) in Species_R7a:
                return 'NBN species present'
            else:
                return 'NBN species not present'
        # Check if the bioregion is 8
        elif bioregion == 'Region 8 (deep-sea)':
            # Check if the relevant species entry is present in the Species_R8 list
            if str(species) in Species_R8:
                return 'NBN species present'
            else:
                return 'NBN species not present'
    elif presence == 'Not Applicable':
        return 'Not Applicable'


# Assign output of the nbn_check() function to a new column titled
# 'Characterising NBN Species Presence'
MR_BiotopesDB_Merge['Characterising NBN Species Presence'] = MR_BiotopesDB_Merge.apply(lambda df: nbn_check(df), axis=1)

# Drop unwanted columns from the DF, retaining only the 'Characterising
# NBN Species Presence' column
MR_BiotopesDB_Merge.drop(['Characterising species in NBN?', 'Species'], axis=1, inplace=True)

#        Rearrange order of columns within the DF
MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[[
    'Bioregion', 'EUNIS_code', 'Present in MR', 'JNCC biotope code',
    'Salinity', 'Wave exposure', 'Tidal streams', 'Substratum', 'Zone',
    'Depth band', 'Other features', 'Description (JNCC, 2015)',
    'Characteristic species', 'Climate', 'Similar biotopes',
    'Link to other biotopes', 'References', 'Comments', 'Region_ID',
    'If present, how many records?', 'Predicted in UK SeaMap?',
    'Characterising NBN Species Presence',
    'L4 parent present (based on data)?', 'Child L5/L6 present?',
    'Similar sibling biotopes present?',
    'Habitat present in literature/survey reports?',
    'Characterising species present in literature/survey reports?',
    'Habitat suitable?', 'Within recorded biotope distribution?',
    'Expert judgement indicates presence?', 'EUNIS Biotopes'
]]

# 4.3.3. Checking if data has a parent L4 biotope present within given location

#        List all L4 biotopes within each bioregion (all data taken from MR samples)
#        Region 1a
L4_R1a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 1a') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 1b
L4_R1b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 1b') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 2: Southern North Sea
L4_R2 = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Region 2: Southern North Sea') &
                             (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 3: Eastern Channel
L4_R3 = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Region 3: Eastern Channel') &
                             (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 4a
L4_R4a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 4a') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 4b
L4_R4b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 4b') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 5a
L4_R5a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 5a') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 5a
L4_R5b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 5b') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 6a
L4_R6a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 6a') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 7a
L4_R7a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 7a') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 7b
L4_R7b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 7b (deep-sea)') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]
#        Region 8
L4_R8 = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Region 8 (deep-sea)') &
                             (MR_Samples_Slice['EUNIS code'].apply(len) == 5)]


#        Define function which iterates through the DF and checks if there is a L4 parent biotope within the given
#        location / bioregion of interest

def l4_check(row):
    # Pull out EUNIS data from DF
    e_code = row['EUNIS_code']
    # Check if EUNIS data is level 5 or 6
    if len(str(e_code)) > 5:
        # If it is L5 or L6, slice the string as fas as L4
        e_slice = e_code[0:5]
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 1a
        if 'Sub-region 1a' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R1a['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 1b
        elif 'Sub-region 1b' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R1b['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Region 2: Southern
        # North Sea
        elif 'Region 2: Southern North Sea' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R2['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Region 3: Eastern
        # Channel
        elif 'Region 3: Eastern Channel' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R3['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 4a
        elif 'Sub-region 4a' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R4a['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 4b
        elif 'Sub-region 4b' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R4b['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 5a
        elif 'Sub-region 5a' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R5a['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 5b
        elif 'Sub-region 5b' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R5b['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 6a
        elif 'Sub-region 6a' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R6a['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 7a
        elif 'Sub-region 7a' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R7a['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 7b
        # (deep-sea)
        elif 'Sub-region 7b (deep-sea)' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R7b['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Region 8 (deep-sea)
        elif 'Region 8 (deep-sea)' in str(row['Bioregion']):
            if e_slice in [x for x in L4_R8['EUNIS code']]:
                return 'L4 parent biotope found'
            else:
                return 'No L4 parent biotope found'
        else:
            return 'Cannot complete process'
        # Check if the L4 slice exists within the preset list of L4 biotopes within each location.
    # If this is not the correct length, return 'Not Applicable
    elif len(str(e_code)) <= 5:
        return 'Not Applicable'


# 4.3.4. Utilise the l4_check function to iterate though the DF and
# calculate if a L4 EUNIS biotope exists within the the same bioregion
# in which the entry is categorised in the DF. This is checked against
# the MR points data which was assigned a given bioregion by
# intersecting geospatial data. The results of this computation are
# stored within the 'L4 parent present (based on data)?' column of the
# MR_BiotopesDB_Merge DF
MR_BiotopesDB_Merge['L4 parent present (based on data)?'] = MR_BiotopesDB_Merge.apply(lambda row: l4_check(row), axis=1)

# 4.3.5. Checking if data has a child L5 biotopes present within a given location / bioregion

#        List all L5 & L6 biotopes within each bioregion (all data taken from MR samples)
#        Region 1a
L56_R1a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 1a') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 1b
L56_R1b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 1b') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 2: Southern North Sea
L56_R2 = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Region 2: Southern North Sea') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 3: Eastern Channel
L56_R3 = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Region 3: Eastern Channel') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 4a
L56_R4a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 4a') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 4b
L56_R4b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 4b') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 5a
L56_R5a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 5a') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 5a
L56_R5b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 5b') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 6a
L56_R6a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 6a') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]

#        Region 7a
L56_R7a = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 7a') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 7b
L56_R7b = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Sub-region 7b (deep-sea)') &
                               (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]
#        Region 8
L56_R8 = MR_Samples_Slice.loc[(MR_Samples_Slice['Bioregion'] == 'Region 8 (deep-sea)') &
                              (MR_Samples_Slice['EUNIS code'].apply(len) >= 6)]


#        Define function which iterates through the DF and checks if there is a L4 parent biotope within the given
#        location / bioregion of interest

def l56_check(row):
    # Pull out EUNIS data from DF
    e_code = row['EUNIS_code']
    # Check if EUNIS data is level 4
    if len(str(e_code)) >= 5:
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 1a
        if 'Sub-region 1a' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R1a['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 1b
        elif 'Sub-region 1b' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R1b['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Region 2: Southern
        # North Sea
        elif 'Region 2: Southern North Sea' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R2['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Region 3: Eastern
        # Channel
        elif 'Region 3: Eastern Channel' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R3['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 4a
        elif 'Sub-region 4a' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R4a['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 4b
        elif 'Sub-region 4b' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R4b['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 5a
        elif 'Sub-region 5a' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R5a['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 5b
        elif 'Sub-region 5b' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R5b['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 6a
        elif 'Sub-region 6a' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R6a['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 7a
        elif 'Sub-region 7a' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R7a['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Sub-region 7b
        # (deep-sea)
        elif 'Sub-region 7b (deep-sea)' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R7b['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        # Check if the L4 row EUNIS code is present within the list correlating to the Bioregion: Region 8 (deep-sea)
        elif 'Region 8 (deep-sea)' in str(row['Bioregion']):
            if any([e_code in x for x in L56_R8['EUNIS code']]):
                return 'L5 / L6 child biotope found'
            else:
                return 'No L5 / L6 child biotope found'
        else:
            return 'Cannot complete process'
        # Check if the L4 slice exists within the preset list of L4 biotopes within each location.
    # If this is not the correct length, return 'Not Applicable
    elif len(str(e_code)) != 5:
        return 'Not Applicable'


# 4.3.6. Utilise the l56_check function to iterate though the DF and
# calculate if a L56 EUNIS biotope exists within the the same bioregion
# in which the entry is categorised in the DF. This is checked against
# the MR points data which was assigned a given bioregion by
# intersecting geospatial data. The results of this computation are
# stored within the 'Child L5/L6 present?' column of the
# MR_BiotopesDB_Merge DF
MR_BiotopesDB_Merge['Child L5/L6 present?'] = MR_BiotopesDB_Merge.apply(lambda row: l56_check(row), axis=1)

# Fill nan values within 'Similar biotopes column with 'Not Applicable'
# - this allows the text check to iterate through these data
MR_BiotopesDB_Merge['Similar biotopes'].fillna('Not Applicable', inplace=True)

# Pull out all UK biotopes into a list within the entire presence
# absence classification
all_biotopes = list(Presence_Absence_Slice['JNCC biotope code'].unique())

# Remove underscores from strings within list

# 4.3.7. Define function which pulls out all bodies of text from the 'Similar biotopes' column of the
# MR_BiotopesDB_Merge DF and adds all biotope codes listed as similar to a list.

def text_checker(row):

    # Pull out main biotope code
    biotope = row['JNCC biotope code']
    # Pull out similar biotopes description
    description = row['Similar biotopes']
    # Tokenize all unique words within body of text
    tokens = nltk.word_tokenize(description)
    # Tag all tokens by word type
    tagged = nltk.pos_tag(tokens)
    # Create empty list to append the words into if they are a biotope code
    biotope_list = [x[0] for x in tagged if x[0] in all_biotopes and x[0] != biotope]
    joined_list = ', '.join(biotope_list)
    if len(biotope_list) == 0:
        return 'Not Applicable'
    else:
        return joined_list


# Execute the text_checker() function to review bodies of text within
# the 'Similar biotopes' column, and return a value dependent on if
# these unique words exist within the UK biotopes classification.
# This new information is stored within the 'Similar sibling biotopes
# present?' column.
MR_BiotopesDB_Merge['Similar sibling biotopes present?'] = MR_BiotopesDB_Merge.apply(lambda row: text_checker(row), axis=1)

########################################################################
# TEXT MINING ANALYSIS IS NOT CURRENTLY IN USE, RETURNS ERRONEOUS
# STRING MATCHES, NEEDS FIXING DO NOT EXECUTE 04/11/2020
########################################################################

#########################
# MPA Searching for text analyses
#########################
#
# # Drop all nan values from bioregions column of the DF
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge.dropna(subset=['Bioregion'])
#
# #       Create dictionary with keys and values
# MPA_dict = {
#     'Sub-region 1a': [
#         'Compass Rose', 'Firth of Forth Banks Complex', 'Southern North Sea', 'Farnes East',
#         'North East of Farnes Deep', 'Swallow Sand', 'Southern Trench', 'Turbot Bank',
#         'Norwegian Boundary Sediment Plain', 'East of Gannet and Montrose Fields', 'Pobie Bank Reef', 'Fulmar'
#     ],
#     'Sub-region 1b': [
#         'Central Fladen', 'Braemar Pockmarks', 'Scanner Pockmark'
#     ],
#     'Region 2: Southern North Sea': [
#         'Kentish Knock East', 'Silver Pit', 'Wash Approach', 'Dogger Bank',
#         'Inner Dowsing, Race Bank and North Ridge', 'North Norfolk Sandbanks and Saturn Reef', "Markham's Triangle",
#         'Holderness Offshore', 'Greater Wash', 'Haisborough, Hammond and Winterton', 'Orford Inshore',
#         'Outer Thames Estuary'
#     ],
#     'Region 3: Eastern Channel': [
#         'South Dorset', 'West of Wight-Barfleur', 'East of Start Point', 'East Meridian', 'Wight-Barfleur Extension',
#         'Bassurelle Sandbank', 'Wight-Barfleur Reef', 'Southern North Sea', 'Offshore Brighton', 'Offshore Overfalls',
#         'Inner Bank', 'East Meridian (Eastern section)', 'Offshore Foreland', 'Foreland'
#     ],
#     'Sub-region 4a': [
#         'South-West Deeps (West)', 'North-West of Jones Bank', 'Greater Haig Fras', 'South West Deeps (East)',
#         'Haig Fras', 'Celtic Deep', 'East of Jones Bank', 'North of Lundy', 'South-East of Falmouth',
#         'Bristol Channel Approaches / Dynesfeydd MÃ´r Hafren', 'East of Haig Fras', 'Western Channel',
#         'South of the Isles of Scilly', 'South of Celtic Deep', 'South West Approaches to the Bristol Channel',
#         'North-East of Haig Fras', 'Cape Bank',
#         'Skomer, Skokholm and the Seas off Pembrokeshire / Sgomer, Sgogwm a Moroedd Penfro', 'North West of Lundy',
#         'East of Celtic Deep', 'West Wales Marine / Gorllewin Cymru Forol', 'North of Celtic Deep'
#     ],
#     'Sub-region 4b': [
#         'The Canyons', 'South West Deeps (East)'
#     ],
#     'Sub-region 5a': [
#         "Mid St George's Channel", 'Mud Hole', 'North of Celtic Deep', 'North Channel',
#         'North Anglesey Marine / Gogledd MÃ´n Forol', 'West of Walney', 'Queenie Corner', 'West of Copeland',
#         'South Rigg', "North St George's Channel", "North St George's Channel Extension", 'Irish Sea Front',
#         'Liverpool Bay', 'Liverpool Bay / Bae Lerpwl', 'Croker Carbonate Slabs',
#         'West Wales Marine / Gorllewin Cymru Forol'
#     ],
#     'Sub-region 5b': [
#         'Slieve Na Griddle', 'Pisces Reef Complex', 'North Channel', 'Queenie Corner'
#     ],
#     'Sub-region 6a': [
#         'Sea of the Hebrides', 'Stanton Banks'
#     ],
#     'Sub-region 7a': [
#         'The Barra Fan and Hebrides Terrace Seamount', 'Geikie Slide and Hebridean Slope', 'Stanton Banks',
#         'West Shetland Shelf', 'North-west Orkney', 'Solan Bank Reef'
#     ],
#     'Sub-region 7b (deep-sea)': [
#         'The Barra Fan and Hebrides Terrace Seamount', 'Geikie Slide and Hebridean Slope',
#         'North-east Faroe-Shetland Channel', 'Faroe-Shetland Sponge Belt', 'Darwin Mounds', 'Wyville Thomson Ridge'
#     ],
#     'Region 8 (deep-sea)': [
#         'Rosemary Bank Seamount', 'Hatton-Rockall Basin', 'The Barra Fan and Hebrides Terrace Seamount',
#         'Geikie Slide and Hebridean Slope', 'Anton Dohrn Seamount', 'East Rockall Bank', 'Hatton Bank',
#         'North West Rockall Bank', 'North-east Faroe-Shetland Channel', 'Faroe-Shetland Sponge Belt', 'Darwin Mounds',
#         'Wyville Thomson Ridge'
#     ]
# }

# # 4.3.8 Pull out all .pdf and Microsoft Office Word files within a targeted folder directory and search files for set
# #       keywords. Develop a dictionary containing the files with of relevance and return this to a new column.
#
# #       Create a memorize function which caches the extracted variables as they are iterated through. This prevents the
# #       files being converted from .pdf and MS Word formats repeatedly.
# def memorize(func):
#     # Create a temporary dictionary to store processed data
#     cache = dict()
#
#     # Define a memorized function which is only executed if the desired result does not already exist within the cache
#     def memorized_func(*args):
#         # Check for the output within the cache
#         if args in cache:
#             # If this already exists, return the existing value
#             return cache[args]
#         # Define a result of the memorized function (this is where the target function is executed)
#         result = func(*args)
#         # Once executed, record this occurrence within the cache
#         cache[args] = result
#         # Return the process of the target function
#         return result
#     # Return the execution of the memorized function
#     return memorized_func
#
#
# #       Create extraction function which will convert the data to text objects to be searched - this will be run as a
# #       memorized version of itself
# def text_extractor(doc_type, dirpath_global, eachfile_global):
#     # Define the required file locality components within the local scope of the function
#     # Each component is defined in a 'for in' loop within the literature_search() function below
#     dirpath_local = dirpath_global
#     eachfile_local = eachfile_global
#
#     # Check if the extension is a .pdf file using the doc_type value defined in the literature_search() function
#     if doc_type == 'PDF':
#         # If the file is a .pdf, open the file using the slate.PDF() method
#         # This must combine the dirpath with eachFile using a raw '\\' string connecting the two elements
#         with open(dirpath_local + r'\\' + eachfile_local, 'rb') as f:
#             pdf_text = slate.PDF(f)
#             return pdf_text
#
#
# #       Convert the text_extractor() function to a memorized version of itself which can utilise caching to limit
# #       duplication of efforts. Therefore, files are only converted from .pdf / MS Word files once, rather than each
# #       time a search is performed
# memorized_text_extractor = memorize(text_extractor)
#
# #       Remove unwanted EUNIS L1 and L2 biotopes from the DF prior to undergoing text analysis
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A1']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A2']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A3']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A4']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A5']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'A6']
# MR_BiotopesDB_Merge = MR_BiotopesDB_Merge[MR_BiotopesDB_Merge['EUNIS_code'] != 'B']
#
#
# #       Define a function which iterates through a target network drive / file path and executes the
# #       memorized_text_extractor() function. This function is applied to a Pandas DF and is computed for every value
# #       within the target column of interest ('EUNIS_Code')
#
# def literature_search_habitat(row):
#     # Define keyword to search the text data on
#
#     search_word_eunis = str(row['EUNIS_code'])
#     search_word_jncc = str(row['JNCC biotope code'])
#     bioregion = str(row['Bioregion'])
#     MPA_searchwords = [x for x in MPA_dict[bioregion]]
#
#     # Create a variable to store all the data where a match is successfully found
#     match_dictionary = {}
#     # Set a directory to be searched through - Survey Data Post 2017 only
#     directory = r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Contracts\C16-0257-105 Biogeographical Regional Contract\LiteratureSearchCopiedReports"
#
#     # Loop through all elements of the  target directory
#     for (dirpath, dirnames, files) in os.walk(directory):
#
#         # Iterate through each item within the files stored within folders and sub-folders
#         for eachFile in files:
#             # Split the iterated item by the file name and extension - retain the extension only through slicing ([1])
#             root_ext = os.path.splitext(eachFile)[1]
#
#             # Check if the extension is a .pdf file
#             if '.pdf' in root_ext:
#
#                 try:
#                     # Run extractor function set to PDF
#                     pdf_text = memorized_text_extractor('PDF', dirpath, eachFile)
#
#                     # Perform search to check if any of the potential MPAs within the associated bioregion exist within
#                     # the literature being searched
#                     if any([x in y for x in MPA_searchwords for y in pdf_text]):
#                         # If the MPA is mentioned within the literature being searched, then preform a check to see if
#                         # the search_word_eunis or the search_word_jncc exist within the converted text.
#                         if any([search_word_eunis in x and search_word_jncc in x for x in pdf_text]):
#                             # Add this successful match to the match_dictionary
#                             match_dictionary[str(dirpath)] = eachFile
#                 except:
#                     # print('Could not process ' + str(eachFile))
#                     pass
#
#     # If there is data within the match_dictionary, return this data as a string value
#     if len(match_dictionary) > 0:
#         # Convert the output into a string format value
#         output = json.dumps(match_dictionary)
#         return output
#     # If no data exists within the match_dictionary, then the search has been unsuccessful and should be stated
#     else:
#         return 'Not found in literature'
#
#
# #       Execute the literature_search() function to review bodies of text within the the target .pdf, and
# #       search them for biotopes of interest This new information is stored within the
# #       'Habitat present in literature/survey reports?' column.
# MR_BiotopesDB_Merge['Habitat present in literature/survey reports?'] =\
#     MR_BiotopesDB_Merge.apply(lambda row: literature_search_habitat(row), axis=1)
#
#
# #       Fill all empty values within the 'Characteristic species' column to 'None'  to avoid empty strings returning
# #       matches erroneously
# MR_BiotopesDB_Merge['Characteristic species'].fillna('No characteristic species', inplace=True)
# MR_BiotopesDB_Merge['Characteristic species'].replace('', 'No characteristic species', inplace=True)
# MR_BiotopesDB_Merge['Characteristic species'].replace(' ', 'No characteristic species', inplace=True)
# MR_BiotopesDB_Merge['Characteristic species'] = MR_BiotopesDB_Merge['Characteristic species'].astype(str)
#
#
# #       Execute the literature_search() function
# def literature_search_spp(row, column):
#     # Define keyword to search the text data on
#     search_word = str(row[column])
#     bioregion = str(row['Bioregion'])
#     MPA_searchwords = [x for x in MPA_dict[bioregion]]
#
#     # Create a variable to store all the data where a match is successfully found
#     match_dictionary = {}
#     # Set a directory to be searched through - Survey Data Post 2017 only
#     directory = r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Contracts\C16-0257-105 Biogeographical Regional Contract\LiteratureSearchCopiedReports"
#
#     # Loop through all elements of the  target directory
#     for (dirpath, dirnames, files) in os.walk(directory):
#
#         # Iterate through each item within the files stored within folders and sub-folders
#         for eachFile in files:
#             # Split the iterated item by the file name and extension - retain the extension only through slicing ([1])
#             root_ext = os.path.splitext(eachFile)[1]
#
#             # Check if the extension is a .pdf file
#             if '.pdf' in root_ext:
#
#                 try:
#
#                     # Run extractor function set to PDF
#                     pdf_text = memorized_text_extractor('PDF', dirpath, eachFile)
#                     # Extract the text from the .pdf file and search the file for the defined keyword
#                     # If the matched pages list is greater than 0, state what has been found
#                     if any([x in y for x in MPA_searchwords for y in pdf_text]):
#                         # If the MPA is mentioned within the literature being searched, then preform a check to see if
#                         # the search_word_eunis or the search_word_jncc exist within the converted text.
#                         if any([search_word in x for x in pdf_text]):
#                             # Add this successful match to the match_dictionary
#                             match_dictionary[str(dirpath)] = eachFile
#                 except:
#                     # print('Could not process ' + str(eachFile))
#                     pass
#
#     # If there is data within the match_dictionary, return this data as a string value
#     if len(match_dictionary) > 0:
#         # Convert the output into a string format value
#         output = json.dumps(match_dictionary)
#         return output
#     # If no data exists within the match_dictionary, then the search has been unsuccessful and should be stated
#     else:
#         return 'Not found in literature'
#
#
# # 4.3.9 Execute the literature_search() function to review bodies of text within the the target .pdf and MS Word, and
# #       search them for species of interest This new information is stored within the
# #       'Characterising species present in literature/survey reports?' column.
# MR_BiotopesDB_Merge['Characterising species present in literature/survey reports?'] =\
#     MR_BiotopesDB_Merge.apply(lambda row: literature_search_spp(row, 'Characteristic species'), axis=1)

# # 4.3.10 Define function which iterates through each record within the 'EUNIS_code' column of the
# #        MR_BiotopesDB_Merge DF, and compiles the keywords / supporting text where the bioregion value relates to the
# #        bioregion column of the BiotopesDB
#
# #        Define function which identifies the keyword combinations from a column of interest
#
# def keyword_extractor(df, column, int_range):
#
#     #############################
#     # Data manipulation
#     #############################
#
#     # Define a list of all words to be removed from further analysis
#
#     cull_words = [
#         'and', 'and', 'jncc', 'biotope', 'description', 'contour', 'contour)', 'based', 'on',
#         ' jncc', 'jncc ', 'JNCC', 'the', 'this', 'as', 'be', 'a', 'by', 'with', 'been', 'very', 'are', 'contour',
#         'from', '#NAME?', 'both', 'up', 'an', 'gases', '(â‰¥', 'at', 'E', 'while', 'although', 'annd',
#         'Agree', 'ansd', 'these', 'along', 'The', 'adjacent', 'baed',  'of',
#         'to', 'in', 'support', 'is', 'part', 'assigned', 'general', 'thin', 'that',
#         'similar', 'levels', 'more', 'oil', 'outside', 'may', 'categorised', 'supplied',
#         'scour', 'project', 'level', 'so', 'due', 'but', 'numbers', 'table', 'requirement', 'large',
#         "gases'", 'Out', 'majority', 'north', 'note', 'soft', 'made', 'shows', 'layer', 'It', 'three',
#         'northern', 'same', 'who', 'most', 'thorughout', 'map', 'it', 'centre', 'agree', 'name'
#     ]
#
#     # Develop a set type object to store all stopwords to be removed from the analysis
#     stop_words = set()
#     # Add custom stopwords to the stopwords list
#     stop_words = stop_words.union(cull_words)
#
#     # Develop text corpus and clean text data of unwanted elements
#     corpus = []
#     for i in range(0, 294):
#         # Remove punctuations
#         text = re.sub('[^a-zA-Z]', ' ', df[column].astype(str)[i])
#         # remove tags
#         text = re.sub("&lt;/?.*?&gt;", " &lt;&gt; ", text)
#         # remove special characters and digits
#         text = re.sub("(\\d|\\W)+", " ", text)
#         # Convert to list from string
#         text = text.split()
#         # Stemming
#         ps = PorterStemmer()
#         # Lemmatisation
#         lem = WordNetLemmatizer()
#         text = [lem.lemmatize(word) for word in text if word not in stop_words]
#         text = " ".join(text)
#         corpus.append(text)
#
#     # The two key parts of this process include Tokenisation and Vectorisation.
#     # To complete this process of text preparation, we utilise the bag of words model, a technique which ignores the
#     #     # sequence of words, and only accounts or the frequencies of occurrence
#
#     # Utilise the sklearn CountVectorizer to tokenise the text and develop a vocabulary of known words.
#     cv = CountVectorizer(
#         max_df=1,
#         # Ignore terms with a document frequency above this threshold (corpus specific words) - not sure if we
#         # want this?
#         stop_words=stop_words,
#         max_features=10000,  # Maximum columns within the matrix
#         ngram_range=(1, int_range))  # Determines the list of words - single, bi-gram and tri-gram word combinations
#
#     # Utilise the fit_transform function to learn and develop the library
#     X = cv.fit_transform(corpus)
#
#     #############################
#     # Data visualisation
#     #############################
#
#     # Develop a data visualisation to represent the most commonly used 4 word sequences
#
#     vec1 = CountVectorizer(ngram_range=(int_range, int_range),
#                            max_features=2000).fit(corpus)
#     bag_of_words = vec1.transform(corpus)
#     sum_words = bag_of_words.sum(axis=0)
#     words_freq = [(word, sum_words[0, idx]) for word, idx in
#                   vec1.vocabulary_.items()]
#     words_freq = sorted(words_freq, key=lambda x: x[1],
#                         reverse=True)
#
#     # Define a DF object containing the top 20 four word combinations
#     ranked_combinations = words_freq[:]
#     ranked_combinations_df = pd.DataFrame(ranked_combinations)
#     ranked_combinations_df.columns = ["Combination", "Freq"]
#     return ranked_combinations_df
#
#
# # Create DF for ranked combinations using 1 words
# SR1a_keywords = keyword_extractor(Biotopes_DB, '1. Northern North Sea ', 2)
# # Create DF for ranked combinations using 2 words
# SNorthSea = keyword_extractor(Biotopes_DB, '2. Southern North Sea Lit Rev', 2)

# DATA ISSUE - literature searches do not work as intended, commented out and all values assigned dummy value
# 'Not found in literature' (LM 04/11/2020)
MR_BiotopesDB_Merge['Habitat present in literature/survey reports?'] = 'Not found in literature'
MR_BiotopesDB_Merge['Characterising species present in literature/survey reports?'] = 'Not found in literature'
# Drop erroneous entries where the numpy nan/float values exist in the 'Bioregions' column
MR_BiotopesDB_Merge.dropna(subset=['Bioregion'], inplace=True)

# 4.3.11 Keyword analysis and Bioregions DB data mining

#        Create a dictionary which stores keywords and all possible Bioregions_DB checks with the set response values
keywords = {
    'Habitat present in literature/survey reports?':
        {
            'sampling results': 'Yes',
            'References': 'Flag for manual check',  # NEEDS TO BE FIXED FOR REFERENCES CHECK - DOES NOT CURRENTLY WORK
            'survey data': 'Flag for manual check',
            'et al': 'Flag for manual check',
            '(': 'Flag for manual check',
            ')': 'Flag for manual check',
            'JNCC map': 'Flag for manual check'
        },
    'Characterising species present in literature/survey reports?':
        {
            'References': 'Flag for manual check',  # NEEDS TO BE FIXED FOR REFERENCES CHECK - DOES NOT CURRENTLY WORK
            'survey data': 'Flag for manual check',
            'et al': 'Flag for manual check',
            '(': 'Flag for manual check',
            ')': 'Flag for manual check',
            'species': 'Flag for manual check',
            'spp': 'Flag for manual check',
        },
        'Habitat suitable?':
        {
            ' suitable': 'Yes',
            'unsuitable': 'No',
            'too shallow': 'No',
            'too deep': 'No',
            'not offshore': 'No',
            r"doesn't occur offshore": 'No',
            'inshore only': 'No',
            'biotope occurs in shallow water': 'No',
            'Not relevant': 'No',
            'NR': 'No',
            'not assessed': 'No',
            'sea lochs': 'No',
            'sealochs': 'No',
            'sea loch': 'No'
        },
    'Within recorded biotope distribution?':
        {
            'outside biogeographic region': 'No',
            'outside known geographic range': 'No',
            'outside geographic area': 'No',
            'outside known distribution': 'No',
            'distribution restricted': 'No',
            'sea lochs': 'No',
            'sealochs': 'No',
            'sea loch': 'No',
            'not recorded': 'Flag for manual check',
            'JNCC map': 'Flag for manual check',
            'within': 'Flag for manual check',
            'outside': 'Flag for manual check',
        },
    'Expert judgement indicates presence?':
        {
            'pers. comm.': 'Flag for manual check',
            'pers comm': 'Flag for manual check',
            ')': 'Flag for manual check',
            '(': 'Flag for manual check',
            'INITIALS?': 'Flag for manual check'  # NEEDS TO BE FIXED - DOES NOT CURRENTLY WORK
        }
}


#        Define function which iterates through each record within the MR_BiotopesDB_Merge DF and categorises data into
#        the 'Habitat suitable?' column, returning a boolean value of Yes or No.
def biotopes_db_search(row, target):

    # Load in EUNIS data / biotope code for target DF
    biotope = row['EUNIS_code']
    bioregion = row['Bioregion']

    # Load in all relevant columns for each bioregion within the Biotopes_DB
    r1a = Biotopes_DB[['EUNIS', '1a Subregion (main)']].astype(str)
    r1b = Biotopes_DB[['EUNIS', '1b. Subregion Fladen Ground']].astype(str)
    r2 = Biotopes_DB[['EUNIS', '2. Southern North Sea Lit Rev']].astype(str)
    r3 = Biotopes_DB[['EUNIS', '3. Eastern Channel: Lit Rev']].astype(str)
    r4a = Biotopes_DB[['EUNIS', ' 4a (main region)']].astype(str)
    r4b = Biotopes_DB[['EUNIS', '4b.  Deep subregion']].astype(str)
    r5a = Biotopes_DB[['EUNIS', '5a Main region']].astype(str)
    r5b = Biotopes_DB[['EUNIS', '5b West of Isle on Man']].astype(str)
    r6a = Biotopes_DB[['EUNIS', '6a. Main subregion ']].astype(str)
    r7a = Biotopes_DB[['EUNIS', '7a Inner']].astype(str)
    r7b = Biotopes_DB[['EUNIS', '7b Outer']].astype(str)

    # Perform check to ensure data being searched is relevant to the
    # bioregion of interest - r1a
    if 'Sub-region 1a' in bioregion:
        r1a_match = r1a.loc[r1a['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r1a_match['1a Subregion (main)'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only
            # be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the
            # list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the
    # bioregion of interest - r1b
    elif 'Sub-region 1b' in bioregion:
        r1b_match = r1b.loc[r1b['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r1b_match['1b. Subregion Fladen Ground'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the
    # bioregion of interest - r2
    elif 'Region 2: Southern North Sea' in bioregion:
        r2_match = r2.loc[r2['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r2_match['2. Southern North Sea Lit Rev'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the
    # bioregion of interest - r3
    elif 'Region 3: Eastern Channel' in bioregion:
        r3_match = r3.loc[r3['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r3_match['3. Eastern Channel: Lit Rev'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the bioregion of interest - r4a
    elif 'Sub-region 4a' in bioregion:
        r4a_match = r4a.loc[r4a['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r4a_match[' 4a (main region)'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the bioregion of interest - r4b
    elif 'Sub-region 4b' in bioregion:
        r4b_match = r4b.loc[r4b['EUNIS'].isin([biotope])]
        match_slice = r4b_match['4b.  Deep subregion'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    elif 'Sub-region 5a' in bioregion:
        r5a_match = r5a.loc[r5a['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r5a_match['5a Main region'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

        # Perform check to ensure data being searched is relevant to the bioregion of interest - r4b
    elif 'Sub-region 5b' in bioregion:
        r5b_match = r5b.loc[r5b['EUNIS'].isin([biotope])]
        match_slice = r5b_match['5b West of Isle on Man'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the bioregion of interest - r6a
    elif 'Sub-region 6a' in bioregion:
        r6a_match = r6a.loc[r6a['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r6a_match['6a. Main subregion '].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the bioregion of interest - r7a
    elif 'Sub-region 7a' in bioregion:
        r7a_match = r7a.loc[r7a['EUNIS'].isin([biotope])]
        match_slice = r7a_match['7a Inner'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'

    # Perform check to ensure data being searched is relevant to the bioregion of interest - r7b
    elif 'Sub-region 7b' in bioregion:
        r7b_match = r7b.loc[r7b['EUNIS'].isin([biotope])]
        # Match slice must be as the location is referred to in the BiotopesDB
        match_slice = r7b_match['7b Outer'].astype(str)
        value = [keywords[target][r] for r in keywords[target] if any(r in y for y in match_slice)]
        # Create unique values within the value list
        value = list(set(value))
        if len(value) == 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return the list joined list as a string (this should only be one item)
            return str(s)
        elif len(value) > 1:
            # Join items within the value list and combine with ', '
            s = ', '.join(value)
            # Return a flag for manual check, with the items within the list as a string in brackets after
            return f'Flag for manual check: ({s})'
        elif len(value) == 0:
            return 'Not found in BiotopesDB records'


# Execute the biotopes_db_search() function on to search the relevant
# column of the Biotopes DB using set keywords and return a value for
# the 'Habitat present in BiotopesDB literature/survey reports?' column
MR_BiotopesDB_Merge['Habitat present in BiotopesDB literature/survey reports?'] =\
    MR_BiotopesDB_Merge.apply(
        lambda row: biotopes_db_search(row, 'Habitat present in literature/survey reports?'), axis=1
    )

# Execute the biotopes_db_search() function on to search the relevant
# column of the Biotopes DB using set keywords and return a value for
# the 'Characterising species present in BiotopesDB literature/survey
# reports?' column
MR_BiotopesDB_Merge['Characterising species present in BiotopesDB literature/survey reports?'] =\
    MR_BiotopesDB_Merge.apply(
        lambda row: biotopes_db_search(row, 'Characterising species present in literature/survey reports?'), axis=1
    )

#     Execute the biotopes_db_search() function on to search the relevant column of the Biotopes DB using set keywords
#     and return a value for the 'Habitat suitable?' column
MR_BiotopesDB_Merge['Habitat suitable?'] =\
    MR_BiotopesDB_Merge.apply(
        lambda row: biotopes_db_search(row, 'Habitat suitable?'), axis=1
    )

#     Execute the biotopes_db_search() function on to search the relevant column of the Biotopes DB using set keywords
#     and return a value for the 'Within recorded biotope distribution?' column
MR_BiotopesDB_Merge['Within recorded biotope distribution?'] =\
    MR_BiotopesDB_Merge.apply(
        lambda row: biotopes_db_search(row, 'Within recorded biotope distribution?'), axis=1
    )


#     Execute the biotopes_db_search() function on to search the relevant column of the Biotopes DB using set keywords
#     and return a value for the 'Expert judgement indicates presence?' column
MR_BiotopesDB_Merge['Expert judgement indicates presence?'] =\
    MR_BiotopesDB_Merge.apply(
        lambda row: biotopes_db_search(row, 'Expert judgement indicates presence?'), axis=1
    )


########################################################################################################################

#                        5. Implementing the final decisions: biotope presence / absence rules                         #

########################################################################################################################
MR_BiotopesDB_Merge['If present, how many records?'] =\
    MR_BiotopesDB_Merge['If present, how many records?'].fillna(0).astype(int)
MR_BiotopesDB_Merge['EUNIS_code'] = MR_BiotopesDB_Merge['EUNIS_code'].astype(str)

# NEW PROBLEM - EUNIS Biotopes column contains 2 EUNIS codes, data not stored within EUNIS_code col

# Create function to decides whether the biotope is determined as present or absent within a given location


def auto_decision(row):
    mrdp = int(row['If present, how many records?'])  # Sorted
    habitat_litreport = row['Habitat present in literature/survey reports?']  # Sorted
    biotopes_db_hab_litreport = row['Habitat present in BiotopesDB literature/survey reports?']  # Sorted
    # eunis = row['EUNIS_code']  # NEEDS TO FIX DOUBLE EUNIS ENTRIES
    child_l56 = row['Child L5/L6 present?']  # Sorted
    uksm = row['Predicted in UK SeaMap?']  # Sorted
    habitat_suitable = row['Habitat suitable?']  # MISSING DATA
    charac_spp_nbn = row['Characterising NBN Species Presence']  # Sorted
    similar_biotopes_pres = row['Similar sibling biotopes present?']
    parent_l4 = row['L4 parent present (based on data)?']  # Sorted
    expert_judge = row['Expert judgement indicates presence?']  # Currently no keywords to indicate present CANNOT WORK?
    charac_spp_litreport = row['Characterising species present in literature/survey reports?']  # Sorted
    biotopes_db_charac_spp_litreport = row['Characterising species present in BiotopesDB literature/survey reports?']  # Sorted
    biotope_distribution = row['Within recorded biotope distribution?']

    # Develop conditions which classify the biotope presence as 'Yes' using the following criteria
    if int(mrdp) >= 2:
        return 'Yes'
    elif habitat_litreport != 'Not found in literature':
        return 'Yes'
    elif biotopes_db_hab_litreport == 'Yes':  # CANNOT BE COMPLETED
        return 'Yes'
    # Check the child_l56 only returns the desired answer (one of 4 possible)
    elif child_l56 == 'L5 / L6 child biotope found':
        return 'Yes'

    elif int(mrdp) < 2\
            and habitat_litreport == 'Not found in literature' \
            and biotopes_db_hab_litreport != 'Yes' \
            and child_l56 != 'L5 / L6 child biotope found':  # POTENTIAL SOURCE OF PROBLEM

        # Can this miss the other L4 check? - ADDED NEW BIOTOPES DB HAB CHECK

        # Develop conditions which classify the biotope presence as 'Possible' using the following criteria
        if int(mrdp) == 1:
            return 'Possible'
        elif int(mrdp) == 0 and uksm == 'Present':
            return 'Possible'
        elif habitat_suitable == 'Yes' and charac_spp_nbn == 'NBN species present':
            return 'Possible'
        elif habitat_suitable == 'Yes' and charac_spp_litreport != 'Not found in literature':
            return 'Possible'
        elif habitat_suitable == 'Yes' and biotopes_db_charac_spp_litreport == 'Yes':
            return 'Possible'
        elif similar_biotopes_pres != 'Not Applicable':
            return 'Possible'
        # Check the parent_l4 only returns the desired answer (one of 4 possible)
        elif parent_l4 == 'L4 parent biotope found':
            return 'Possible'
        elif expert_judge == 'Present':  # NOT CURRENTLY EXECUTED ACCURATELY
            return 'Possible'

        elif int(mrdp) == 0\
                and uksm == 'Absent'\
                and habitat_litreport == 'Not found in literature'\
                and biotopes_db_hab_litreport != 'Yes'\
                and charac_spp_litreport == 'Not found in literature'\
                and biotopes_db_charac_spp_litreport != 'Yes'\
                and charac_spp_nbn != 'NBN species present'\
                and expert_judge != 'Present':  # CAN PASS THIS STAGE

            # Develop conditions which classify the biotope presence as 'Unlikely' using the following criteria
            if int(mrdp) == 0\
                    and uksm == 'Absent'\
                    and habitat_litreport == 'Not found in literature'\
                    and biotopes_db_hab_litreport != 'Yes'\
                    and charac_spp_litreport == 'Not found in literature' \
                    and biotopes_db_charac_spp_litreport != 'Yes' \
                    and habitat_suitable == 'Yes':
                return 'Unlikely'  # CANNOT BE COMPLETED CURRENTLY

            elif int(mrdp) == 0\
                    and uksm == 'Absent'\
                    and habitat_litreport == 'Not found in literature'\
                    and biotopes_db_hab_litreport != 'Yes'\
                    and charac_spp_litreport == 'Not found in literature' \
                    and biotopes_db_charac_spp_litreport != 'Yes' \
                    and biotope_distribution == 'Yes':
                return 'Unlikely'

            elif int(mrdp) == 0\
                    and uksm != 'Absent'\
                    and habitat_litreport != 'Not found in literature'\
                    and biotopes_db_hab_litreport != 'Yes'\
                    and habitat_suitable != 'Yes':

                # Develop conditions which classify the biotope presence as 'No' using the following criteria=
                if habitat_suitable != 'Yes':
                    return 'No'
                elif charac_spp_nbn != 'NBN species present'\
                        and charac_spp_litreport == 'Not found in literature'\
                        and biotopes_db_charac_spp_litreport != 'Yes':
                    return 'No'
                elif biotope_distribution == 'No':
                    return 'No'

    else:
        return 'Cannot complete process'


# Run the auto_decision() function on the df to return a result to a new column
MR_BiotopesDB_Merge['Result'] = MR_BiotopesDB_Merge.apply(lambda row: auto_decision(row), axis=1)

##################################

# Export MR_BiotopesDB_Merge for audit trail of work

# Define file name to save, categorised by date
filename = "MR_BiotopesDB_Merge_" + (time.strftime("%Y%m%d") + ".xlsx")
# Run the output DF.to_csv method
MR_BiotopesDB_Merge.to_excel(outpath + filename,  sheet_name='MR_BiotopesDB_Merge')


########################################################################################################################

#                6. Comparing existing 2017 Bioregions Contract outputs with newly created automated results           #

########################################################################################################################

# Load output to write final piece of code - updates the original 2017 contract output where there has been new
# evidence, otherwise the 2017 result is kept as final.
BiotopesAuto_DF = MR_BiotopesDB_Merge

# Import existing bioregions data to provide inshore/offshore overview
existing_bioregions2017 = pd.read_excel("./Bioregions/Data/Bioregions_extract_AccessQRY_20200430.xlsx", 'Qry_Bioregional_Gaps')

# Refine the existing_bioregions DF
existing_bioregions2017 = existing_bioregions2017[['SubregionName', 'BiotopePresence', 'HabitatCode']]

# Rename the columns within the existing_bioregions column to enable a merge with the BiotopesAutoDF
existing_bioregions2017.columns = ['Bioregion', 'BiotopePresence', 'EUNIS_code']

# Create comparison of automated / existing bioregions extract DFs
NewBioExistingBioMerge = pd.merge(BiotopesAuto_DF, existing_bioregions2017, on=['Bioregion', 'EUNIS_code'], how='outer')

# Rename columns within the NewBioExistingBioMerge DF to clearly indicate the result of the Bioregion automation in
# contrast to the existing Bioregions 2017 contract presence / absence output
NewBioExistingBioMerge.rename(columns={'Result': 'NewBioResult', 'BiotopePresence': 'ExistingBioResult'}, inplace=True)

# Fill nan values within 'NewBioResult' column with 'Not Applicable' - this allows the text check to iterate
# through these data
NewBioExistingBioMerge['NewBioResult'].fillna('Not Applicable', inplace=True)


# Define function which can be applied to the entire DF,
def comparison(row):
    # Pull out all existing biotope presence / absence data
    existing = row['ExistingBioResult']
    # Pull out all newly created biotope presence / absence based on automated analyses
    automated = row['NewBioResult']

    # Create series of conditional statements to
    if 'Yes' in automated:
        return 'Yes'
    elif 'Yes' not in automated:
        return existing


# Run the comparison() function to create a new column value 'UpdatedBiotopePresence' which retains the existing 2017
# Bioregions presence/absence contract output, unless new evidence acquired via automated analyses indicates presence
# ('Yes')
NewBioExistingBioMerge['UpdatedBiotopePresence'] = NewBioExistingBioMerge.apply(lambda row: comparison(row), axis=1)

##################################

# Export NewBioExistingBioMerge for audit trail of work

# Define file name to save, categorised by date
filename = "NewBioExistingBioMerge_" + (time.strftime("%Y%m%d") + ".xlsx")
# Run the output DF.to_csv method
NewBioExistingBioMerge.to_excel(outpath + filename,  sheet_name='NewBioExistingBioMerge')

########################################################################################################################

#                  7. Creating a new Bioregions extract, formatted for use within the MarESA aggregation               #

########################################################################################################################

# Create a new DF which only includes the columns of interest required within the bioregions extract
NewBioExtract = NewBioExistingBioMerge[['Bioregion', 'UpdatedBiotopePresence', 'EUNIS_code']]

# Rename columns within NewBioExtract DF
NewBioExtract.columns = ['SubregionName', 'BiotopePresence', 'HabitatCode']

# Import existing biotopes extract to provide inshore/offshore overview
OldBioExtract = pd.read_excel("./Bioregions/Data/Bioregions_extract_AccessQRY_20200430.xlsx", 'Qry_Bioregional_Gaps')

# Refine the existing_bioregions DF to only retain inshore/offshore details and EUNIS code
OldBioExtract = OldBioExtract[['BiotopePresence', 'HabitatCode']]

# Refine OldBioExtract DF to comprise inshore only biotopes
OldBioExtractInshore = OldBioExtract.loc[OldBioExtract['BiotopePresence'].isin(['Inshore only'])]

# Merge existing and current bioregions data into a single DF
BioExMergeInshoreOffshore = pd.merge(NewBioExtract, OldBioExtractInshore, how='outer', on='HabitatCode')

# Use inshore only data to populate left bio_merge 'BiotopePresence' column entries
BioExMergeInshoreOffshore.loc[BioExMergeInshoreOffshore['BiotopePresence_y'] == 'Inshore only', 'BiotopePresence_x'] = 'Inshore only'

# Drop unwanted column 'BiotopePresence_y' from DF
BioExMergeInshoreOffshore.drop(['BiotopePresence_y'], axis=1, inplace=True)

# Rename columns as required for aggregation process
UpdatedBioExtract = BioExMergeInshoreOffshore
UpdatedBioExtract.columns = ['SubregionName', 'BiotopePresence', 'HabitatCode']

# Create dummy data entries for the HabitatName and Gaps columns - these are also redundant, but are referred to
# repeatedly throughout all aggregation scripts. Therefore, it was less time consuming to create dummy columns than
# edit out all instances where these are referenced in the aggregation scripts.
UpdatedBioExtract['HabitatName'] = 'DummyData'
UpdatedBioExtract['Gaps'] = 'DummyData'

##################################

# Removing any data which have been flagged as erroneous before undertaking any further analyses to create the
# bioregions extract for the MareESA Aggregation.
# Define list of biotopes to exclude from DF

biotopes = [
    'A1.1131', 'A1.123', 'A1.311', 'A1.3122', 'A1.3141', 'A1.3142', 'A1.3151', 'A1.412', 'A1.421', 'A2.611', 'A3.1112',
    'A5.5211', 'A5.53', 'B3.111', 'B3.1132'
]

# Refine the DF, ensuring that the listed biotopes are not present within the EUNIS_code column of the DF
UpdatedBioExtract = UpdatedBioExtract.loc[~UpdatedBioExtract['HabitatCode'].isin(biotopes)]

##################################

# Define file name to save, categorised by date
filename = "BioregionsExtract_" + (time.strftime("%Y%m%d") + ".xlsx")
# Run the output DF.to_csv method
UpdatedBioExtract.to_excel(outpath + filename,  sheet_name='BioregionsExtract')

########################################################################################################################

# Stop the timer post computation and print the elapsed time
elapsed = (time.process_time() - start)

# Create print statement to indicate how long the process took and round value to 1 decimal place.
print('This script took ' + str(round(elapsed / 60, 1)) + ' minutes to run and complete.' + '\n' +
      '(' + str(round(round(elapsed / 60, 1) / 60, 1)) + ' hours to run and complete' + ')' + '\n' +
      '(' + str(round(round(round(elapsed / 60, 1) / 60, 1) / 24, 2)) + ' days to run and complete' + ')'
      )

