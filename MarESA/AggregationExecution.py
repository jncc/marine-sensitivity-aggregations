########################################################################

# Title: Aggregation Execution

# Authors: Matear, L.(2020), Fox, K (2021)
# Email: marinepressures@jncc.gov.uk
# Version Control: 2.0

# Script description: Execution script required to run the JNCC MarESA
# Aggregations. Please review this file prior to
# running it to confirm all desired aggregations are listed.
# For any enquiries please contact marinepressures@jncc.gov.uk

# Change the names of the new mar-ESA extract file and then let all
# the scripts run. This script runs all the rest of the scripts


########################################################################
#
#                       A. Aggregation Preparation:
#
########################################################################

# Import all Python libraries required or data manipulation
import os
import sys
import time
from pathlib import Path

# Test the run time of the function
start = time.process_time()

# Set the overall directory
os.chdir('C://Users//Ollie.Grint//Documents//marine-sensitivity-aggregations')

# set the output file
output_file= '20240206v2/'
Path("./MarESA/Output/"+output_file).mkdir(parents=True, exist_ok=True)

########################################################################
#
#           Broad-Scale Habitat Aggregations (BSH)
#
########################################################################

# CHECK  FOR MOST UP-TO-DATE MarESA EXTRACT VERSION:
# The top copy can be found at the following file path:
# \\jncc-corpfile\gis\Reference\Marine\Sensitivity
marESA_file = 'MarESA-Data-Extract-habitatspressures_2023_11_07.csv'
# Excel tab names change dependent on the data received from the MBA,
# therefore, the input MarESA Extract Excel
# marESA_tab = 'MarESA-Data-extract_2021-07-02'

#marESA_file = 'MarESA-Data-extract_2021-07-02.csv'
#marESA_tab = 'MarESA-Data-extract_2021-07-02'

# This is name of the file from the bioregions script
bioregions_ext = 'BioregionsExtract_20220310.xlsx'

# correlation table for the offshore aggregation script
cor_table = 'CorrelationTable_C16042020.xlsx'
# cor_table = '201801_EUNIS07and04_to_JNCC15.03andListed_CorrelationTable.xlsx'

EnglishOffshore = 'English_Offshore_FOCI&BSH_2022-03-16.csv'
WelshBSH = 'Welsh_Inshore_BSH_2022-03-16.csv'
WelshFOCI = 'Welsh_Inshore_FOCI_2022-03-16.csv'
EngWel_Annex1 = 'English_Welsh_Offshore_AnnexI_2022-03-16.csv'
Scot_Annex1 = 'Scottish_Offshore_AnnexI_2022-05-06.csv'
Scot_PMF = 'Scottish_Offshore_PMF_2022-04-29.csv'

# These names will be replaced with the output of a script
# if the script isn't being run, enter the output of the offshore
# resistance and resilience scripts here
offshore_res_file = 'OffshoreResAgg_20220316_Bioreg20220310_marESA20220310.csv'
offshore_resil_file = 'OffshoreResilAgg_20220316_Bioreg20220310_marESA20220310.csv'

# This part is for adding directory names to the path so that all the
# scripts can be found
# finding all the subdirectories in current folder
dirs = [name for name in os.listdir('./MarESA/Scripts/')
            if os.path.isdir(os.path.join('./MarESA/Scripts/', name))]
# removing the Data folder
rem_dirs = ['Data', 'Output']
dirs = [dd for dd in dirs if dd not in rem_dirs]
# adding each of the directories to the path
for dd in dirs:
    sys.path.append('./MarESA/Scripts/' + dd)

print('\n\n')

########################################################################
#
#                   Off-shore aggregation
#
########################################################################

########################################################################
# Title: JNCC MarESA Sensitivity Aggregation (EUNIS)

import SensitivityAggregationOffshore as SAO
SAO.main(marESA_file, bioregions_ext,output_file)

# ########################################################################
# # Title: JNCC MarESA Resistance Aggregation (EUNIS)

import ResistanceAggregationOffshore as RtAO
# running the resistance script
# getting the resistance output file name to use later in the bs3 script
offshore_res_file = RtAO.main(marESA_file, bioregions_ext,output_file)

# ########################################################################
# # Title: JNCC MarESA Resilience Aggregation (EUNIS)

import ResilienceAggregationOffshore as RcAO
# running the script offshore resilience script
# getting the resilience output file name to use later in the bs3 script
offshore_resil_file = RcAO.main(marESA_file, bioregions_ext,output_file)

########################################################################
#
#                  Deep Sea Bed aggregation
#
########################################################################

########################################################################
# Title: Deep Seabed Sensitivity Aggregation

import DeepSeabed_Sens_Agg as DSA
DSA.main(marESA_file, EnglishOffshore,output_file)

# ########################################################################
# # Title: Deep Seabed Resilience Aggregation

# import DeepSeabed_Resil_Agg as DRA
# DRA.main(marESA_file, EnglishOffshore)
# Present in Canyons MCZ?

# ########################################################################
# # Title: MCZ Wales Inshore Broadscale Habitat Sensitivity Aggregation

import MCZ_Wales_In_BSH_Sens_Agg as MWB
MWB.main(marESA_file, WelshBSH,output_file)

########################################################################
#
#                 OSPAR: Common Indicator Extent of Physical
#               Damage to predominant and special habitats (BH3)
#
########################################################################

#############################################################
# Title: OSPAR BH3 Sensitivity Calculation

# import BH3_SensitivityCalculation as BH3SC
# BH3SC.main(offshore_res_file, offshore_resil_file,output_file)

########################################################################
#
#                 MCZ Feature of Conservation Importance
#                           (FOCI) Aggregations
#
########################################################################

#############################################################
# MCZ Offshore FeatureOfConservationImportance (FOCI) Sensitivity Aggregation

import MCZ_Off_FOCI_Sens_Agg as MOFS
MOFS.main(marESA_file, EnglishOffshore,output_file)

# #############################################################
# # MCZ Offshore Feature of Conservation Importance (FOCI) Resilience Aggregation

# import MCZ_Off_FOCI_Resil_Agg as MOFR
# MOFR.main(marESA_file, EnglishOffshore)
# ['Habitat of Conservation Importance (HOCI)', 'Sub-split: Bioregion?', 'Biotope name'] not in index"

# #############################################################
# # MCZ Wales Inshore Feature of Conservation Importance Sensitivity Aggregation

import MCZ_Wales_In_FOCI_Sens_Agg as MWIFC
MWIFC.main(marESA_file, WelshFOCI,output_file)

########################################################################
#
#                Habitats Directive Annex I Aggregations
#
########################################################################

#############################################################
# Title: Annex I England and Wales Offshore Sensitivity Aggregation

import AnxI_EngWales_Off_Sens_Agg as AEWOS
AEWOS.main(marESA_file, EngWel_Annex1,output_file)

# #############################################################
# # Title: Annex I England and Wales Offshore Resilience Aggregation

# import AnxI_EngWales_Off_Resil_Agg as AEWOR
# AEWOR.main(marESA_file, EngWel_Annex1,output_file)
#['Subregion', 'Annex I sub feature type', 'Biotope name']

# #############################################################
# # Title: Annex I  Scotland Inshore & Offshore Sensitivity Aggregation

# SHOUD NOT WORK - old one
# import AnxI_Scot_InOff_Sens_Agg as ASIOS
# ASIOS.main(marESA_file, Scot_Annex1,output_file)
# df1.loc[:, '_tmpkey'] = 1

#############################################################
# Title: Annex I  Scotland Offshore Sensitivity Aggregation

# SHOUDL WORK
import AnxI_Scot_Off_Sens_Agg as ASOS
ASOS.main(marESA_file, Scot_Annex1,output_file)

########################################################################
#
#            NCMPA Priority Marine Feature (PMF Aggregation)
#
########################################################################

#############################################################
# Title: PMF Offshore Sensitivity Aggregation

import PMF_Off_Sens_Agg_ExDepth as POSAED
POSAED.main(marESA_file, Scot_PMF,output_file)

########################################################################
#
#                           Aggregation Audit Log
#
########################################################################

# Execute the Aggregation Audit Log
# Run alongside a QA script and a file send script
import AggregationAuditLog as audit
#audit.main(audit = True, send = True)

# Stop the timer post computation and print the elapsed time
elapsed = (time.process_time() - start)

# Create print statement to indicate how long the process took and
# round value to 1 decimal place.
# print("The 'AggregationExecution' script took " + str(
#     round(elapsed / 60, 1)) + ' minutes to run and complete.')
