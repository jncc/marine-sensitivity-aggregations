########################################################################

# Title: Aggregation Audit Log

# Authors: Matear, L.(2020), Fox, K (2021)
# Email: marinepressures@jncc.gov.uk
# Version Control: 2.0

# Script description: Create an audit trail of the data created from
# the MarESA Agregations and the relevant input
# datasets used.

# This code is executed automatically when the AggregationExecution.py
# script is run.

# For any enquiries please contact marinepressures@jncc.gov.uk

########################################################################
#
#  A. MarESA Preparation: Unknowns Automation
#
########################################################################

# Import libraries used within the script, assign a working directory
# and import data

# Import all Python libraries required
import os
import time
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'

from os import listdir
from os.path import isfile, join
from shutil import copyfile

#############################################################


# Define the code as a function to be executed as necessary
def main(audit = True, send = False):

    print('Audit script starting...\n')

    output_files = [f for f in listdir('./MarESA/Output/') if isfile(join('./MarESA/Output/', f))]

    # Create empty list to store the file names for the most recently
    # created MarESA output files
    files_list = []
    files_date = []
    files_bioregion = []
    files_maresa = []
    files_resistance = []
    files_resilience = []

    # The Follwoing part of the script looks for each possible output
    # file in the outputs folder, finds the most up-to-date verion of
    # each and extracts the verison control information

    ####################################################################
    # Broad-Scale Habitat Aggregations (BSH)
    ####################################################################

    #############################################################
    # Title: JNCC MarESA Sensitivity Aggregation (EUNIS)

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'OffshoreSensAgg' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path or .csv
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[1])
        files_bioregion.append(file_name.split('_')[2])
        files_maresa.append(file_name.split('_')[3])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Sensitivity\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMarESA sensitivity file not found.\n'))


    #############################################################
    # Title: JNCC MarESA Resistance Aggregation (EUNIS)

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'OffshoreResAgg' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[1])
        files_bioregion.append(file_name.split('_')[2])
        files_maresa.append(file_name.split('_')[3])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resistance\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMarESA resistance file not found.\n'))

    #############################################################
    # Title: JNCC MarESA Resilience Aggregation (EUNIS)

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'OffshoreResilAgg' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[1])
        files_bioregion.append(file_name.split('_')[2])
        files_maresa.append(file_name.split('_')[3])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resilience\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMarESA resilience file not found.\n'))

    #############################################################
    # Title: Deep Seabed Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'DeepSeabed_Sens_Agg' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\MCZ_Off\DeepSeabed_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nDeep sea sensitivity file not found.\n'))

    #############################################################
    # Title: Deep Seabed Resilience Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'DeepSeabed_Resil_Agg' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\MCZ_Off\DeepSeabed_Resil_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nDeep sea sensitivity file not found.\n'))

    #############################################################
    # Title: MCZ Wales Inshore Broadscale Habitat Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'MCZ_Wales_In_BSH' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\MCZ_Wales_In\BSH_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMCZ Wales Inshore Broadscale Habitat' +
        ' Sensitivity Aggregation file not found.\n'))

    #############################################################
    # Title: OSPAR BH3 Sensitivity Calculation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'BH3_OffSens' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[2])
        files_bioregion.append('Not Applicable')
        files_maresa.append('Not Applicable')
        files_resistance.append(file_name.split('_')[3])
        files_resilience.append(file_name.split('_')[4])

        if send:
            destination_folder = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\BH3Calculations"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nOSPAR BH3 Sensitivity Calculation' +
        ' file not found.\n'))

    ####################################################################
    # MCZ Feature of Conservation Importance (FOCI) Aggregations
    ####################################################################

    #############################################################
    # Title: MCZ Offshore FOCI Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'MCZ_Off_FOCI_Sens' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\MCZ_Off\FOCI_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMCZ_Off_FOCI_Sens Calculation' +
        ' file not found.\n'))

    #############################################################
    # Title: MCZ Offshore FOCI Resilience Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'MCZ_Off_FOCI_Resil' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\MCZ_Off\FOCI_Resil_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMCZ_Off_FOCI_Resil Calculation' +
        ' file not found.\n'))

    #############################################################
    # MCZ Wales Inshore FOCI Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'MCZ_Wales_In_FOCI' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\MCZ_Wales_In\FOCI_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nMCZ_Wales_In_FOCI Calculation' +
        ' file not found.\n'))

    ####################################################################
    # Habitats Directive Annex I Aggregations
    ####################################################################

    #############################################################
    # Title: Annex I England and Wales Offshore Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'AnxI_EngWales_Off_Sens' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\AnxI_EngWales_Off\AnxI_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nAnxI_EngWales_Off_Sens Calculation' +
        ' file not found.\n'))

    #############################################################
    # Title: Annex I England and Wales Offshore Resilience Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'AnxI_EngWales_Off_Resil' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\AnxI_EngWales_Off\AnxI_Resil_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nAnxI_EngWales_Off_Resil Calculation' +
        ' file not found.\n'))

    #############################################################
    # Title: Annex I  Scotland Inshore & Offshore Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'AnxI_Scot_In&Off_Sens' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            #destination_folder = r"\\jncc-corpfile\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\AnxI_Scot_In&Off\AnxI_Sens_Agg\\"
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\AnxI_Scot_In&Off\AnxI_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nAnxI_Scot_In&Off_Sens Calculation' +
        ' file not found.\n'))

    #############################################################
    # Title: Annex I  Scotland Offshore Sensitivity Aggregation

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'AnxI_Scot_Off_Sens' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\AnxI_Scot_In&Off\AnxI_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nAnxI_Scot_Off_Sens Calculation' +
        ' file not found.\n'))

    #############################################################
    # Title: PMF Offshore sensitivity

    try:
        # Finding all the files with the string in them
        maresaSens = [f for f in output_files if 'PMF_Off_Sens' in f]
        # Adding the file paths to the file names so they can be found
        maresaSensP = [os.path.join('./MarESA/Output/', f) for f in maresaSens]
        # Ordering the foudn file paths by modification date to get the
        # most up-to-date file just created (creation date is
        # operating system specific so hasn't been used)
        maresaSensP.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Getting the filename back without the path
        fn = maresaSensP[0].split('/')[-1]
        file_name= fn[:fn.index('.')]

        # Extracting the relevant bits of info out of the filename
        files_list.append(fn)
        files_date.append(file_name.split('_')[-2])
        files_bioregion.append('Not Applicable')
        files_maresa.append(file_name.split('_')[-1])
        files_resistance.append('Not Applicable')
        files_resilience.append('Not Applicable')

        if send:
            destination_folder = r"\\jncc-corpfile\JNCC Corporate Data\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Mapping\Sensitivity aggregations\Feature_level\NCMPA_Off\PMF_Off_Sens_Agg\\"
            copyfile(maresaSensP[0], destination_folder + fn)
            print(maresaSensP[0] + ' sent to ' + destination_folder)
    except:
        ValueError (print('\nPMF_Off_Sens Calculation' +
        ' file not found.\n'))

    ####################################################################
    # Combining into a dataframe and extracting versions and dates
    ####################################################################

    # Load the newest MarESA Aggregation output files as a Pandas
    # Dataframe with the column set to 'File Name'

    new_files = pd.DataFrame({
        'File Name': files_list,
        'Date Created': files_date,
        'Bioregions Extract Used': files_bioregion,
        'MarESA Extract Used': files_maresa,
        'Resistance Aggregation Used': files_resistance,
        'Resilience Aggregation Used': files_resilience
        })

    #############################################################

    if audit:
        # Load in the existing log file to be updated with the outputs to
        # store metadata
        audit = pd.read_csv("./MarESA/MarESAAggregation_OutputLog_AuditTrailOnly.csv")

        # Append the newly created files back into the existing audit DF
        updated_audit = audit.append(new_files)

        updated_audit = updated_audit[[
            'File Name', 'Date Created', 'Bioregions Extract Used',
            'MarESA Extract Used', 'Resistance Aggregation Used',
            'Resilience Aggregation Used'
        ]]

        # Export the audit trail document back to the original filepath
        # once updated
        updated_audit.to_csv('./MarESA/MarESAAggregation_OutputLog_AuditTrailOnly.csv',
                             sep=',',
                             index=False)

    # Create print statement to indicate how long the process took and
    # round value to 1 decimal place.
    print('\n...The audit script has finished running.')

if __name__ == "__main__":
    os.chdir('C:/Users/Ollie.Grint/Documents')
    main(audit = False, send = True)
