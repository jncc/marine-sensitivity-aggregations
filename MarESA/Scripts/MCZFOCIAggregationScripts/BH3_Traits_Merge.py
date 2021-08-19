########################################################################################################################

# Title: BH3 Traits Collation

# Authors: S. Marra, Matear L., 2021                                                  Email:
# Version Control: 0.1

# Script description:

#                        For any enquiries please contact XXXX

########################################################################################################################

#                                               A. BH3 Traits Collation                                                #

########################################################################################################################

# Type alt + shift + e when code is highlighted to run it in the console

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required
import pandas as pd
import time

#############################################################

# Load all spreadsheets for use in script

# Load main file “Python trial_Trait collation.xlsx” as Pandas DataFrame object
# Convert file path to raw string type using the 'r' in front of the string object - for example, please see:
# https://chercher.tech/python-programming/python-special-characters for special characters
TraitsCollationDF = pd.read_excel(r"\\jncc-corpfile\gis\GISprojects\Marine\MSFD\Physical_Damage_Indicator\_working\Sensitivity tests\SM_BH3 sensitivity trial\Sensitivity Tables for trial\Python programming trial\Python trial_Trait collation.xlsx", header=1)

# Load in data from TACOI_Stage_1 - data stored in
TraitsTacoi = pd.read_excel(r"\\jncc-corpfile\gis\GISprojects\Marine\MSFD\Physical_Damage_Indicator\_working\Sensitivity tests\SM_BH3 sensitivity trial\Sensitivity Tables for trial\Python programming trial\BH1_SIMPER_Traits_v0.2.xlsx", 'Traits_KW_SM')

# Load in data from Biogenic
TraitsBiogenic = pd.read_excel(r"\\jncc-corpfile\gis\GISprojects\Marine\MSFD\Physical_Damage_Indicator\_working\Sensitivity tests\SM_BH3 sensitivity trial\Sensitivity Tables for trial\Python programming trial\Trait analysis_SACFOR_V0.1AS_LM.xlsx", 'Trait working')

#############################################################

# Clean TraitsTacoi DF prior to merge
TraitsTacoiClean = TraitsTacoi[[
    'Species', 'Size', 'Reference', 'Comment', 'Trait Category', 'BS1', 'Longevity', 'Reference.1', 'Comment.1',
    'Trait Category.1', 'BS2', 'Motility', 'Reference.2', 'comment', 'Trait Category.2', 'BS3', 'Attachment',
    'Reference.3', 'Comment.2', 'Trait Category.3', 'BS4', 'Benthic position', 'Reference.4', 'Comment.3',
    'Trait Category.4', 'BS5', 'Flexibility', 'Reference.5', 'Comment.4', 'Trait Category.5', 'BS6',
    'Fragility', 'Reference.6', 'Comment.5', 'Trait Category.6', 'BS7', 'Feeding habits', 'Reference.7',
    'Comment.6', 'Trait Category.7', 'BS8', 'OVERALL SENSITIVITY SCORE (to trawling fishing)']]


# Run merge between TraitsCollationDF and TraitsTacoi using pd.merge(leftDF, rightDF, lefton, righton, how)
TraitsMergeTacoi = pd.merge(TraitsCollationDF, TraitsTacoi, how='outer', on='Species')

# Clean TraitsBiogenic DF prior to merge
TraitsBiogenic = TraitsBiogenic[[
    'Species', 'Size', 'Reference', 'Comment', 'Trait Category', 'BS1', 'Longevity', 'Reference.1', 'Comment.1',
    'Trait Category.1', 'BS2', 'Motility', 'Reference.2', 'Comment.2', 'Trait Category.2', 'BS3', 'Attachment',
    'Reference.3', 'Comment.3', 'Trait Category.3', 'BS4', 'Benthic position', 'Reference.4', 'Comment.4',
    'Trait Category.4', 'BS5', 'Flexibility', 'Reference.5', 'Comment.5', 'Trait Category.5', 'BS6', 'Fragility',
    'Reference.6', 'Comment.6', 'Trait Category.6', 'BS7', 'Feeding habits', 'Reference.7', 'Comment.7',
    'Trait Category.7', 'BS8', 'OVERALL SENSITIVITY SCORE (to trawling fishing)']]

# Run merge between Traits
TraitsMergeTacoiBiogenic = pd.merge(TraitsMergeTacoi, TraitsBiogenic, how='outer', on='Species')

# # Output data for view in Excel - use DataFrame.to_csv('filepath/filename.csv', sep=',')
# TraitsMergeTacoiBiogenic.to_csv(r'\\jncc-corpfile\gis\GISprojects\Marine\MSFD\Physical_Damage_Indicator\_working\Sensitivity tests\SM_BH3 sensitivity trial\Sensitivity Tables for trial\Python programming trial\output.csv', sep=',')

# Export DF for use

# Define folder file path to be saved into
outpath = r"\\jncc-corpfile\gis\GISprojects\Marine\MSFD\Physical_Damage_Indicator\_working\Sensitivity tests\SM_BH3 sensitivity trial\Sensitivity Tables for trial\Python programming trial"
# Define file name to save, categorised by date
filename = "BH3TraitsMerge_" + (time.strftime("%Y%m%d") + ".csv")
# Run the output DF.to_csv method
TraitsMergeTacoiBiogenic.to_csv(outpath + "\\" + filename, sep=',')