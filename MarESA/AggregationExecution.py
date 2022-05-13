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

import os
import sys
import time

# Test the run time of the function
start = time.process_time()

########################################################################
#                          File names

# CHECK  FOR MOST UP-TO-DATE MarESA EXTRACT VERSION:
# \\jncc-corpfile\gis\Reference\Marine\Sensitivity
marESA_file = 'MarESA-Data-Extract-habitatspressures_2022-04-20.csv'
# marESA_tab = 'MarESA-Data-extract_2021-07-02'

# This is name of the file from the bioregions script
bioregions_ext = 'BioregionsExtract_20220310.xlsx'

# correlation table for the offshore aggregation script
cor_table = 'CorrelationTable_C16042020.xlsx'

EnglishOffshore = 'English_Offshore_FOCI&BSH_2022-03-16.csv'
WelshBSH = 'Welsh_Inshore_BSH_2022-03-16.csv'
WelshFOCI = 'Welsh_Inshore_FOCI_2022-03-16.csv'
EngWel_Annex1 = 'English_Welsh_Offshore_AnnexI_2022-03-16.csv'
Scot_Annex1 = 'Scottish_Offshore_AnnexI_2022-05-06.csv'
Scot_PMF = 'Scottish_Offshore_PMF_2022-04-29.csv'

########################################################################
#                          Scripts

import SensitivityAggregationOffshore as SAO
SAO.main(marESA_file, bioregions_ext)

import ResistanceAggregationOffshore as RtAO
offshore_res_file = RtAO.main(marESA_file, bioregions_ext)

import ResilienceAggregationOffshore as RcAO
offshore_resil_file = RcAO.main(marESA_file, bioregions_ext)

import DeepSeabed_Sens_Agg as DSA
DSA.main(marESA_file, EnglishOffshore)

import MCZ_Wales_In_BSH_Sens_Agg as MWB
MWB.main(marESA_file, WelshBSH)

import MCZ_Off_FOCI_Sens_Agg as MOFS
MOFS.main(marESA_file, EnglishOffshore)

import MCZ_Wales_In_FOCI_Sens_Agg as MWIFC
MWIFC.main(marESA_file, WelshFOCI)

import AnxI_EngWales_Off_Sens_Agg as AEWOS
AEWOS.main(marESA_file, EngWel_Annex1)

import AnxI_Scot_Off_Sens_Agg as ASOS
ASOS.main(marESA_file, Scot_Annex1)

import PMF_Off_Sens_Agg_ExDepth as POSAED
POSAED.main(marESA_file, Scot_PMF)

########################################################################
#                           Aggregation Audit Log

# Execute the Aggregation Audit Log
# Run alongside a QA script and a file send script
# import AggregationAuditLog as audit
# audit.main(audit = True, send = False)

# Stop the timer post computation and print the elapsed time
elapsed = (time.process_time() - start)

# Create print statement to indicate how long the process took and
# round value to 1 decimal place.
# print("The 'AggregationExecution' script took " + str(
#     round(elapsed / 60, 1)) + ' minutes to run and complete.')
