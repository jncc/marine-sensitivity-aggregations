########################################################################################################################

# Title: Scottish Annex I Join

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
Scot_Anx1_Off = pd.read_excel(r'Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\FeAST\Annex1_FeAST\Trial_AnnexI_sub-types\AnnexI_sub_types_all_v6.xlsx', 'L5_BiotopesForAgg_2Reef')

# Read in the second DF - Annex 1 Sub-types
bioregions = pd.read_excel(r'J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Aggregation_InputData\BioregionsExtract\BioregionsExtract_20201105.xlsx')

# Refine bioregions data to only include rows of interest
bioregionsrefined = bioregions.drop(bioregions[bioregions['BiotopePresence'] == 'No'].index, inplace=False)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['BiotopePresence'] == 'Inshore only'].index, inplace=True)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['SubregionName'] == 'Region 2: Southern North Sea'].index, inplace=True)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['SubregionName'] == 'Region 3: Eastern Channel'].index, inplace=True)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['SubregionName'] == 'Sub-region 4a'].index, inplace=True)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['SubregionName'] == 'Sub-region 4b'].index, inplace=True)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['SubregionName'] == 'Sub-region 5a'].index, inplace=True)
bioregionsrefined.drop(bioregionsrefined[bioregionsrefined['SubregionName'] == 'Sub-region 5b'].index, inplace=True)
# Refine bioregions to not include 'nan' in the BiotopePresence
bioregionsrefined.dropna(subset=['BiotopePresence'], inplace=True)

# Check column names in either DataFrame
list(Scot_Anx1_Off)
list(bioregions)

# Full join all data by EUNIS Code - Version 1
#leftjoin = pd.merge(Scot_Anx1_Off, bioregionsrefined, left_on='EUNIS code', right_on='HabitatCode', how='left')

# Full join all data by EUNIS Code - Version 2
outerjoin = pd.merge(Scot_Anx1_Off, bioregionsrefined, left_on='EUNIS code', right_on='HabitatCode', how='outer',
                     indicator=True)

# Slice the outerjoin DF to only retain data which is in both left and right tables
relevantdata = outerjoin.loc[outerjoin['_merge'].isin(['both'])]

# Refine DF to only include columns of interest
regionsScotAnx1biotopes = relevantdata[
    ['Annex I Habitat', 'Annex I sub-type', 'Depth zone', 'Classification level',
     'EUNIS code', 'Biotope name', 'JNCC code', 'JNCC name', 'SubregionName', 'BiotopePresence']
]

# Duplicate information from 'SubregionName' column to 'Sub-split: Bioregion?'
regionsScotAnx1biotopes['Sub-split: Bioregion?'] = regionsScotAnx1biotopes['SubregionName']

# Drop unwanted data from the regionsScotAnx1biotopes DF
regionsScotAnx1biotopes.drop(['SubregionName'], axis=1, inplace=True)

# Define folder file path to be saved into
outpath = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\AnxI_Scot_In&Off\Method"
# Define file name to save, categorised by date
filename = "regionsScotAnx1biotopes" + ".csv"
# Run the output DF.to_csv method
regionsScotAnx1biotopes.to_csv(outpath + "\\" + filename, sep=',')