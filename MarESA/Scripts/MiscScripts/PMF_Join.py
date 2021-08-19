########################################################################################################################

# Title: PMF Join

# Authors: Matear, L. & Robson L (2020)
# Version Control: 1.0

# Script description:
#
#                        For any enquiries please contact

########################################################################################################################

# Import all libraries required to complete processes
import pandas as pd
# Example data types
# String data
string = 'words'
# Boolean data
yesno = True
# Integer data
integer = 1
# Floating point
floating = 1.4
# List
mylist = [1, 2, 4, 6]
# Dictionary
mydictionary = {'mykey': 'value1',
                'secondkey': 'value2'}

# Read the first DF - MarESA Special Extract
pmf_offshore = pd.read_excel(r'Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\PMFs\NCMPA_PMF_Offshore.xlsx', 'BiotopeFeatureCorrelation_Off')

# Read in the second DF - Annex 1 Sub-types
bioregions = pd.read_excel(r'J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Aggregation_InputData\BioregionsExtract\BioregionsExtract_20201105.xlsx')

# Refine bioregions data to only include rows of interest
bioregionsrefined = bioregions.drop(bioregions[bioregions['BiotopePresence'] == 'No'].index, inplace=False)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['BiotopePresence'] == 'Inshore only'].index, inplace=True)

# Refine bioregions to not include 'nan' in the BiotopePresence
bioregionsrefined.dropna(subset=['BiotopePresence'], inplace=True)

# Check column names in either DataFrame
list(pmf_offshore)
list(bioregions)

# Full join all data by EUNIS Code - Version 1
leftjoin = pd.merge(pmf_offshore, bioregionsrefined, left_on='EUNIS code', right_on='HabitatCode', how='left')

# Full join all data by EUNIS Code - Version 2
outerjoin = pd.merge(pmf_offshore, bioregionsrefined, left_on='EUNIS code', right_on='HabitatCode', how='outer',
                     indicator=True)

# Slice the outerjoin DF to only retain data which is in both left and right tables
relevantdata = outerjoin.loc[outerjoin['_merge'].isin(['both'])]

# Refine DF to only include columns of interest
regionsPMFbiotopes = relevantdata[
    ['Priority Marine Feature (PMF)', 'Sub-split: Depth', 'Classification level',
     'EUNIS code', 'Biotope name', 'JNCC code', 'JNCC name', 'SubregionName', 'BiotopePresence']
]

# Duplicate information from 'SubregionName' column to 'Sub-split: Bioregion?'
regionsPMFbiotopes['Sub-split: Bioregion?'] = regionsPMFbiotopes['SubregionName']

# Drop unwanted data from the regionsPMFbiotopes DF
regionsPMFbiotopes.drop(['SubregionName'], axis=1, inplace=True)

# Export the DF to .csv format
regionsPMFbiotopes.to_csv(r'Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\PMFs\regionsPMFbiotopes.csv',
                          sep=',')
