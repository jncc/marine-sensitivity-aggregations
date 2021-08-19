########################################################################################################################

# Title: OSPAR BH3 Sensitivity Calculation

# Authors: Matear, L.(2019)                                                           Email: marinepressures@jncc.gov.uk
# Version Control: 1.0

# Script description:    Calculate sensitivity following the OPSPAR BH3 Indicator method. This Python script uses
#                        Resistance and Resilience outputs from the JNCC MarESA Aggregation.

#                        For any enquiries please contact marinepressures@jncc.gov.uk

########################################################################################################################

#                                           BH3 Sensitivity Calculations:                                              #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required
import os
import time
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


# Create function to execute main script
def main(resistance_file, resilience_file):

    start = time.process_time()
    print('BH3 sensitivity script started...')

    # Import aggregated resistance data

    # Run short analysis to identify the most recently created
    # iteration of the Resistance Aggregation and read this in
    # for the BH3 process

    # Define a directory to be searched
    #res_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resistance"
    # Set this as the working directory
    #os.chdir(res_dir)
    # Read in all files within this target directory
    #res_files = filter(os.path.isfile, os.listdir(res_dir))
    # Add full filepaths to the identified files within said directory
    #res_files = [os.path.join(res_dir, f) for f in res_files]
    # Sort all files read in by the most recently edited first
    #res_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Define the bioregions object containing the updated outputs
    # from the Bioregions 2017 Contract - read the last
    # edited file
    #resistance_masterframeOFF = pd.read_csv(res_files[0], dtype=str)

    resistance_masterframeOFF = pd.read_csv('./Output/' + resistance_file,
        dtype=str)

    ############################################################

    # Create an object variable storing the date of the bioregions
    # version used - this is entered into the aggregation
    # output file name for QC purposes.
    res_version = str(resistance_file).split('.')[0]
    # Re-split the file name to only retain the date of creation
    res_version = str(res_version).split('_')[1]
    # Create an abreviated version of the filename with the date
    res_version = 'ResAgg' + str(res_version)

    ############################################################
    # Run short analysis to identify the most recently created iteration of the Resilience Aggregation and read this in
    # for the BH3 process

    # Define a directory to be searched
    #resil_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\MarESA_AggregationOutputs_Main\MarESAAggregationOutputs\Resilience"
    # Set this as the working directory
    #os.chdir(resil_dir)
    # Read in all files within this target directory
    #resil_files = filter(os.path.isfile, os.listdir(resil_dir))
    # Add full filepaths to the identified files within said directory
    #resil_files = [os.path.join(resil_dir, f) for f in resil_files]
    # Sort all files read in by the most recently edited first
    #resil_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Define the bioregions object containing the updated outputs from the Bioregions 2017 Contract - read the last
    # edited file
    #resilience_masterframeOFF = pd.read_csv(resil_files[0], dtype=str)

    resilience_masterframeOFF = pd.read_csv('./Output/' + resilience_file,
        dtype=str)

    ############################################################

    # Create an object variable storing the date of the bioregions version used - this is entered into the aggregation
    # output file name for QC purposes.
    # Re-split the string to remove the .csv file extension
    resil_version = str(resilience_file).split('.')[0]
    # Re-split the file name to only retain the date of creation
    resil_version = str(resil_version).split('_')[1]
    # Create an abreviated version of the filename with the date
    resil_version = 'ResilAgg' + str(resil_version)

    ############################################################

    # Subset the resistance_masterframe to only retain the required values for the BH3 sensitivity calculation
    # process
    resistance_masterframeOFF = resistance_masterframeOFF[[
        'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_AggregationConfidenceValue',
        'L2_AggregationConfidenceScore', 'Level_3', 'L3_FinalResistance', 'L3_AggregationConfidenceValue',
        'L3_AggregationConfidenceScore', 'Level_4', 'L4_FinalResistance', 'L4_AggregationConfidenceValue',
        'L4_AggregationConfidenceScore', 'Level_5', 'L5_FinalResistance', 'L5_AggregationConfidenceValue',
        'L5_AggregationConfidenceScore', 'Level_6', 'L6_FinalResistance', 'L6_AggregationConfidenceValue',
        'L6_AggregationConfidenceScore'
    ]]

    # Subset the resilience_masterframe to only retain the required values for the BH3 sensitivity calculation
    # process
    resilience_masterframeOFF = resilience_masterframeOFF[[
        'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResilience', 'L2_AggregationConfidenceValue',
        'L2_AggregationConfidenceScore', 'Level_3', 'L3_FinalResilience', 'L3_AggregationConfidenceValue',
        'L3_AggregationConfidenceScore', 'Level_4', 'L4_AggregationConfidenceValue',
        'L4_AggregationConfidenceScore', 'L4_FinalResilience', 'Level_5', 'L5_FinalResilience',
        'L5_AggregationConfidenceValue', 'L5_AggregationConfidenceScore', 'Level_6', 'L6_FinalResilience',
        'L6_AggregationConfidenceValue', 'L6_AggregationConfidenceScore'
    ]]

    # Create a merged single DF which stores the relevant resistance and resilience information for each
    # biotope/pressure interaction
    res_resil_mergeOFF = pd.merge(resistance_masterframeOFF, resilience_masterframeOFF, how='outer', on=[
        'Pressure', 'SubregionName', 'Level_2', 'Level_3', 'Level_4', 'Level_5', 'Level_6'
    ])

    # Drop trailing whitespace from string values required for the calculation
    res_resil_mergeOFF['L5_FinalResistance'] = res_resil_mergeOFF['L2_FinalResistance'].str.strip()
    res_resil_mergeOFF['L5_FinalResistance'] = res_resil_mergeOFF['L3_FinalResistance'].str.strip()
    res_resil_mergeOFF['L5_FinalResistance'] = res_resil_mergeOFF['L4_FinalResistance'].str.strip()
    res_resil_mergeOFF['L5_FinalResistance'] = res_resil_mergeOFF['L5_FinalResistance'].str.strip()
    res_resil_mergeOFF['L5_FinalResistance'] = res_resil_mergeOFF['L6_FinalResistance'].str.strip()

    res_resil_mergeOFF['L5_FinalResilience'] = res_resil_mergeOFF['L2_FinalResilience'].str.strip()
    res_resil_mergeOFF['L5_FinalResilience'] = res_resil_mergeOFF['L3_FinalResilience'].str.strip()
    res_resil_mergeOFF['L5_FinalResilience'] = res_resil_mergeOFF['L4_FinalResilience'].str.strip()
    res_resil_mergeOFF['L5_FinalResilience'] = res_resil_mergeOFF['L5_FinalResilience'].str.strip()
    res_resil_mergeOFF['L5_FinalResilience'] = res_resil_mergeOFF['L6_FinalResilience'].str.strip()

    # Create code which corrects the erroneous data supplied within the MarESA extract prior to running the BH3
    # calculations. This is completed through DF .loc() with multiple conditions - where EUNIS code is X, and pressure
    # is Y, resistance or resilience value becomes Z

    # The errors to be fixed are:

    # A5.332 - Introduction of light, resistance should be ‘high’, not ‘no evidence’
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A5.332') &
        (res_resil_mergeOFF['Pressure'] == 'Introduction of light') &
        (res_resil_mergeOFF['L5_FinalResistance'] == 'No evidence'),
        'L5_FinalResistance'] = 'High'

    # A1.111 - Electromagnetic changes, resistance should be ‘no evidence’, not high.
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A1.111') &
        (res_resil_mergeOFF['Pressure'] == 'Electromagnetic changes') &
        (res_resil_mergeOFF['L5_FinalResistance'] == 'High'),
        'L5_FinalResistance'] = 'No evidence'

    # A3.223 – Physical change (to another sediment type), resilience should be ‘Not relevant’ instead of Very low.
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A3.223') &
        (res_resil_mergeOFF['Pressure'] == 'Physical change (to another sediment type)') &
        (res_resil_mergeOFF['L5_FinalResilience'] == 'Very low'),
        'L5_FinalResilience'] = 'Not relevant'

    # A5.222 - Salinity increase, resilience should be ‘high’ not ‘No evidence’ and overall sensitivity should be ‘Low’
    # instead of ‘no evidence’
    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A5.222') &
        (res_resil_mergeOFF['Pressure'] == 'Salinity increase') &
        (res_resil_mergeOFF['L5_FinalResilience'] == 'No evidence'),
        'L5_FinalResilience'] = 'High'

    res_resil_mergeOFF.loc[
        (res_resil_mergeOFF['Level_5'] == 'A5.222') &
        (res_resil_mergeOFF['Pressure'] == 'Salinity increase') &
        (res_resil_mergeOFF['L5_FinalResilience'] == 'No evidence'),
        'L5_FinalSensitivity'] = 'Low'

    ############################################################

    # Define function which takes a range and returns the most precautionary value
    def precautionary_value(row, assessment_type, column):
        # Process the most precautionary value from all resistance scores
        if assessment_type == 'Resistance':
            res = str(row[column])
            if 'None' in res:
                return 'None'
            elif 'None' not in res and 'Low' in res:
                return 'Low'
            elif 'None' not in res and 'Low' not in res and 'Medium' in res:
                return 'Medium'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' in res:
                return 'High'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' in res:
                return 'Not relevant'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' not in res and 'Not assessed' in res:
                return 'Unassessed'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' not in res and 'Not assessed' not in res and 'Unknown' in res:
                return 'Unassessed'
            elif 'None' not in res and 'Low' not in res and 'Medium' not in res and 'High' not in res and \
                    'Not relevant' not in res and 'Not assessed' not in res and 'Unknown' not in res and \
                    'No evidence' in res:
                return 'Unassessed'

        # Process the most precautionary value from all resistance scores
        # NOTE: precautionary values for resilience do not currently account for 'Very High' as this value is not
        # used within JNCC data
        elif assessment_type == 'Resilience':
            resil = str(row[column])
            if 'Very low' in resil:
                return 'Very low'
            elif 'Very low' not in resil and 'Low' in resil:
                return 'Low'
            elif 'Very low' not in resil and 'Low' not in resil and 'Medium' in resil:
                return 'Medium'
            elif 'Very low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' in resil:
                return 'High'
            elif 'Very low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' in resil:
                return 'Not relevant'
            elif 'Very low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' not in resil and 'Not assessed' in resil:
                return 'Unassessed'
            elif 'Very low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' not in resil and 'Not assessed' not in resil and 'Unknown' in resil:
                return 'Unassessed'
            elif 'Very low' not in resil and 'Low' not in resil and 'Medium' not in resil and 'High' not in resil \
                    and 'Not relevant' not in resil and 'Not assessed' not in resil and 'Unknown' not in resil and \
                    'No evidence' in resil:
                return 'Unassessed'

# LM: Not in use 09/04/21
    # # Run precautionary assessment and assign to new column for Resistance assessments L2-L5
    # # EUNIS L2
    # res_resil_mergeOFF['L2_ResPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resistance', 'L2_FinalResistance'), axis=1)
    # # EUNIS L3
    # res_resil_mergeOFF['L3_ResPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resistance', 'L3_FinalResistance'), axis=1)
    # # EUNIS L4
    # res_resil_mergeOFF['L4_ResPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resistance', 'L4_FinalResistance'), axis=1)
    # # EUNIS L5
    # res_resil_mergeOFF['L5_ResPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resistance', 'L5_FinalResistance'), axis=1)
    # # EUNIS L6
    # res_resil_mergeOFF['L6_ResPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resistance', 'L6_FinalResistance'), axis=1)
    #
    # # Run precautionary assessment and assign to new column for Resilience assessments L2-L5
    # # EUNIS L2
    # res_resil_mergeOFF['L2_ResilPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resilience', 'L2_FinalResilience'), axis=1)
    # # EUNIS L3
    # res_resil_mergeOFF['L3_ResilPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resilience', 'L3_FinalResilience'), axis=1)
    # # EUNIS L4
    # res_resil_mergeOFF['L4_ResilPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resilience', 'L4_FinalResilience'), axis=1)
    # # EUNIS L5
    # res_resil_mergeOFF['L5_ResilPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resilience', 'L5_FinalResilience'), axis=1)
    # # EUNIS L6
    # res_resil_mergeOFF['L6_ResilPrecaution'] = \
    #     res_resil_mergeOFF.apply(lambda row: precautionary_value(row, 'Resilience', 'L6_FinalResilience'), axis=1)

# LM TO FIX 09/04/21

    # # Define function to iterate through the DF and assign a BH3 sensitivity value based on the resistance and
    # # resilience score - LM TO FIX NOT CURRENTLY WORKING
    # def bh3_calc(row, eunislvl):
    #     # Run calculations for EUNIS level 2 aggregations
    #     # Load necessary columns into the function
    #     # EUNIS L2
    #     L2_ResPrec = str(res_resil_mergeOFF['L2_ResPrecaution'])
    #     L2_ResilPrec = str(res_resil_mergeOFF['L2_ResilPrecaution'])
    #     # EUNIS L3
    #     L3_ResPrec = str(res_resil_mergeOFF['L3_ResPrecaution'])
    #     L3_ResilPrec = str(res_resil_mergeOFF['L3_ResilPrecaution'])
    #     # EUNIS L4
    #     L4_ResPrec = str(res_resil_mergeOFF['L4_ResPrecaution'])
    #     L4_ResilPrec = str(res_resil_mergeOFF['L4_ResilPrecaution'])
    #     # EUNIS L5
    #     L5_ResPrec = str(res_resil_mergeOFF['L5_ResPrecaution'])
    #     L5_ResilPrec = str(res_resil_mergeOFF['L5_ResilPrecaution'])
    #     # EUNIS L6
    #     L6_ResPrec = str(res_resil_mergeOFF['L6_ResPrecaution'])
    #     L6_ResilPrec = str(res_resil_mergeOFF['L6_ResilPrecaution'])
    #
    #     if eunislvl == 2:
    #         # Conduct calculation if the value is 'None'
    #         if L2_ResPrec == 'None' and \
    #                 L2_ResilPrec == 'Very Low':
    #             return 5
    #         elif L2_ResPrec == 'None' and \
    #                 L2_ResilPrec == 'Low':
    #             return 4
    #         elif L2_ResPrec == 'None' and \
    #                 L2_ResilPrec == 'Medium':
    #             return 4
    #         elif L2_ResPrec == 'None' and \
    #                 L2_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the value is 'Low'
    #         elif L2_ResPrec == 'Low' and \
    #                 L2_ResilPrec == 'Very Low':
    #             return 4
    #         elif L2_ResPrec == 'Low' and \
    #                 L2_ResilPrec == 'Low':
    #             return 4
    #         elif L2_ResPrec == 'Low' and \
    #                 L2_ResilPrec == 'Medium':
    #             return 3
    #         elif L2_ResPrec == 'Low' and \
    #                 L2_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the value is 'Medium'
    #         elif L2_ResPrec == 'Medium' and \
    #                 L2_ResilPrec == 'Very Low':
    #             return 4
    #         elif L2_ResPrec == 'Medium' and \
    #                 L2_ResilPrec == 'Low':
    #             return 3
    #         elif L2_ResPrec == 'Medium' and \
    #                 L2_ResilPrec == 'Medium':
    #             return 3
    #         elif L2_ResPrec == 'Medium' and \
    #                 L2_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the value is 'High'
    #         elif L2_ResPrec == 'High' and \
    #                 L2_ResilPrec == 'Very Low':
    #             return 3
    #         elif L2_ResPrec == 'High' and \
    #                 L2_ResilPrec == 'Low':
    #             return 3
    #         elif L2_ResPrec == 'High' and \
    #                 L2_ResilPrec == 'Medium':
    #             return 2
    #         elif L2_ResPrec == 'High' and \
    #                 L2_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the value is 'Not relevant'
    #         elif L2_ResPrec == 'Not relevant' and \
    #                 L2_ResilPrec == 'Not relevant':
    #             return -2
    #
    #         # Conduct calculation if the value is 'Not assessed'
    #         elif L2_ResPrec == 'Not assessed' and \
    #                 L2_ResilPrec == 'Not assessed':
    #             return -1
    #
    #         # Conduct calculation if the value is 'No evidence'
    #         elif L2_ResPrec == 'Not evidence' and \
    #                 L2_ResilPrec == 'Not evidence':
    #             return -1
    #
    #         # Conduct calculation if the value is 'Unknown'
    #         elif L2_ResPrec == 'Unknown' and \
    #                 L2_ResilPrec == 'Unknown':
    #             return -1
    #
    #     # Run calculations for EUNIS level 3 aggregations
    #     elif eunislvl == 3:
    #         # Conduct calculation if the resistance value is 'None'
    #         if L3_ResPrec == 'None' and \
    #                 L3_ResilPrec == 'Very Low':
    #             return 5
    #         elif L3_ResPrec == 'None' and \
    #                 L3_ResilPrec == 'Low':
    #             return 4
    #         elif L3_ResPrec == 'None' and \
    #                 L3_ResilPrec == 'Medium':
    #             return 4
    #         elif L3_ResPrec == 'None' and \
    #                 L3_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Low'
    #         elif L3_ResPrec == 'Low' and \
    #                 L3_ResilPrec == 'Very Low':
    #             return 4
    #         elif L3_ResPrec == 'Low' and \
    #                 L3_ResilPrec == 'Low':
    #             return 4
    #         elif L3_ResPrec == 'Low' and \
    #                 L3_ResilPrec == 'Medium':
    #             return 3
    #         elif L3_ResPrec == 'Low' and \
    #                 L3_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Medium'
    #         elif L3_ResPrec == 'Medium' and \
    #                 L3_ResilPrec == 'Very Low':
    #             return 4
    #         elif L3_ResPrec == 'Medium' and \
    #                 L3_ResilPrec == 'Low':
    #             return 3
    #         elif L3_ResPrec == 'Medium' and \
    #                 L3_ResilPrec == 'Medium':
    #             return 3
    #         elif L3_ResPrec == 'Medium' and \
    #                 L3_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the resistance value is 'High'
    #         elif L3_ResPrec == 'High' and \
    #                 L3_ResilPrec == 'Very Low':
    #             return 3
    #         elif L3_ResPrec == 'High' and \
    #                 L3_ResilPrec == 'Low':
    #             return 3
    #         elif L3_ResPrec == 'High' and \
    #                 L3_ResilPrec == 'Medium':
    #             return 2
    #         elif L3_ResPrec == 'High' and \
    #                 L3_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the value is 'Not relevant'
    #         elif L3_ResPrec == 'Not relevant' and \
    #                 L3_ResilPrec == 'Not relevant':
    #             return -2
    #
    #         # Conduct calculation if the value is 'Not assessed'
    #         elif L3_ResPrec == 'Not assessed' and \
    #                 L3_ResilPrec == 'Not assessed':
    #             return -1
    #
    #         # Conduct calculation if the value is 'No evidence'
    #         elif L3_ResPrec == 'Not evidence' and \
    #                 L3_ResilPrec == 'Not evidence':
    #             return -1
    #
    #         # Conduct calculation if the value is 'Unknown'
    #         elif L3_ResPrec == 'Unknown' and \
    #                 L3_ResilPrec == 'Unknown':
    #             return -1
    #
    #     # Run calculations for EUNIS level 4 aggregations
    #     elif eunislvl == 4:
    #         # Conduct calculation if the resistance value is 'None'
    #         if L4_ResPrec == 'None' and \
    #                 L4_ResilPrec == 'Very Low':
    #             return 5
    #         elif L4_ResPrec == 'None' and \
    #                 L4_ResilPrec == 'Low':
    #             return 4
    #         elif L4_ResPrec == 'None' and \
    #                 L4_ResilPrec == 'Medium':
    #             return 4
    #         elif L4_ResPrec == 'None' and \
    #                 L4_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Low'
    #         elif L4_ResPrec == 'Low' and \
    #                 L4_ResilPrec == 'Very Low':
    #             return 4
    #         elif L4_ResPrec == 'Low' and \
    #                 L4_ResilPrec == 'Low':
    #             return 4
    #         elif L4_ResPrec == 'Low' and \
    #                 L4_ResilPrec == 'Medium':
    #             return 3
    #         elif L4_ResPrec == 'Low' and \
    #                 L4_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Medium'
    #         elif L4_ResPrec == 'Medium' and \
    #                 L4_ResilPrec == 'Very Low':
    #             return 4
    #         elif L4_ResPrec == 'Medium' and \
    #                 L4_ResilPrec == 'Low':
    #             return 3
    #         elif L4_ResPrec == 'Medium' and \
    #                 L4_ResilPrec == 'Medium':
    #             return 3
    #         elif L4_ResPrec == 'Medium' and \
    #                 L4_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the resistance value is 'High'
    #         elif L4_ResPrec == 'High' and \
    #                 L4_ResilPrec == 'Very Low':
    #             return 3
    #         elif L4_ResPrec == 'High' and \
    #                 L4_ResilPrec == 'Low':
    #             return 3
    #         elif L4_ResPrec == 'High' and \
    #                 L4_ResilPrec == 'Medium':
    #             return 2
    #         elif L4_ResPrec == 'High' and \
    #                 L4_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the value is 'Not relevant'
    #         elif L4_ResPrec == 'Not relevant' and \
    #                 L4_ResilPrec == 'Not relevant':
    #             return -2
    #
    #         # Conduct calculation if the value is 'Not assessed'
    #         elif L4_ResPrec == 'Not assessed' and \
    #                 L4_ResilPrec == 'Not assessed':
    #             return -1
    #
    #         # Conduct calculation if the value is 'No evidence'
    #         elif L4_ResPrec == 'Not evidence' and \
    #                 L4_ResilPrec == 'Not evidence':
    #             return -1
    #
    #         # Conduct calculation if the value is 'Unknown'
    #         elif L4_ResPrec == 'Unknown' and \
    #                 L4_ResilPrec == 'Unknown':
    #             return -1
    #
    #     # Run calculations for EUNIS level 5 aggregations
    #     elif eunislvl == 5:
    #         # Conduct calculation if the resistance value is 'None'
    #         if L5_ResPrec == 'None' and \
    #                 L5_ResilPrec == 'Very Low':
    #             return 5
    #         elif L5_ResPrec == 'None' and \
    #                 L5_ResilPrec == 'Low':
    #             return 4
    #         elif L5_ResPrec == 'None' and \
    #                 L5_ResilPrec == 'Medium':
    #             return 4
    #         elif L5_ResPrec == 'None' and \
    #                 L5_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Low'
    #         elif L5_ResPrec == 'Low' and \
    #                 L5_ResilPrec == 'Very Low':
    #             return 4
    #         elif L5_ResPrec == 'Low' and \
    #                 L5_ResilPrec == 'Low':
    #             return 4
    #         elif L5_ResPrec == 'Low' and \
    #                 L5_ResilPrec == 'Medium':
    #             return 3
    #         elif L5_ResPrec == 'Low' and \
    #                 L5_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Medium'
    #         elif L5_ResPrec == 'Medium' and \
    #                 L5_ResilPrec == 'Very Low':
    #             return 4
    #         elif L5_ResPrec == 'Medium' and \
    #                 L5_ResilPrec == 'Low':
    #             return 3
    #         elif L5_ResPrec == 'Medium' and \
    #                 L5_ResilPrec == 'Medium':
    #             return 3
    #         elif L5_ResPrec == 'Medium' and \
    #                 L5_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the resistance value is 'High'
    #         elif L5_ResPrec == 'High' and \
    #                 L5_ResilPrec == 'Very Low':
    #             return 3
    #         elif L5_ResPrec == 'High' and \
    #                 L5_ResilPrec == 'Low':
    #             return 3
    #         elif L5_ResPrec == 'High' and \
    #                 L5_ResilPrec == 'Medium':
    #             return 2
    #         elif L5_ResPrec == 'High' and \
    #                 L5_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the value is 'Not relevant'
    #         elif L5_ResPrec == 'Not relevant' and \
    #                 L5_ResilPrec == 'Not relevant':
    #             return -2
    #
    #         # Conduct calculation if the value is 'Not assessed'
    #         elif L5_ResPrec == 'Not assessed' and \
    #                 L5_ResilPrec == 'Not assessed':
    #             return -1
    #
    #         # Conduct calculation if the value is 'No evidence'
    #         elif L5_ResPrec == 'Not evidence' and \
    #                 L5_ResilPrec == 'Not evidence':
    #             return -1
    #
    #         # Conduct calculation if the value is 'Unknown'
    #         elif L5_ResPrec == 'Unknown' and \
    #                 L5_ResilPrec == 'Unknown':
    #             return -1
    #
    #     # Run calculations for EUNIS level 6 aggregations
    #     elif eunislvl == 6:
    #         # Conduct calculation if the resistance value is 'None'
    #         if L6_ResPrec == 'None' and \
    #                 L6_ResilPrec == 'Very Low':
    #             return 5
    #         elif L6_ResPrec == 'None' and \
    #                 L6_ResilPrec == 'Low':
    #             return 4
    #         elif L6_ResPrec == 'None' and \
    #                 L6_ResilPrec == 'Medium':
    #             return 4
    #         elif L6_ResPrec == 'None' and \
    #                 L6_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Low'
    #         elif L6_ResPrec == 'Low' and \
    #                 L6_ResilPrec == 'Very Low':
    #             return 4
    #         elif L6_ResPrec == 'Low' and \
    #                 L6_ResilPrec == 'Low':
    #             return 4
    #         elif L6_ResPrec == 'Low' and \
    #                 L6_ResilPrec == 'Medium':
    #             return 3
    #         elif L6_ResPrec == 'Low' and \
    #                 L6_ResilPrec == 'High':
    #             return 3
    #
    #         # Conduct calculation if the resistance value is 'Medium'
    #         elif L6_ResPrec == 'Medium' and \
    #                 L6_ResilPrec == 'Very Low':
    #             return 4
    #         elif L6_ResPrec == 'Medium' and \
    #                 L6_ResilPrec == 'Low':
    #             return 3
    #         elif L6_ResPrec == 'Medium' and \
    #                 L6_ResilPrec == 'Medium':
    #             return 3
    #         elif L6_ResPrec == 'Medium' and \
    #                 L6_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the resistance value is 'High'
    #         elif L6_ResPrec == 'High' and \
    #                 L6_ResilPrec == 'Very Low':
    #             return 3
    #         elif L6_ResPrec == 'High' and \
    #                 L6_ResilPrec == 'Low':
    #             return 3
    #         elif L6_ResPrec == 'High' and \
    #                 L6_ResilPrec == 'Medium':
    #             return 2
    #         elif L6_ResPrec == 'High' and \
    #                 L6_ResilPrec == 'High':
    #             return 2
    #
    #         # Conduct calculation if the value is 'Not relevant'
    #         elif L6_ResPrec == 'Not relevant' and \
    #                 L6_ResilPrec == 'Not relevant':
    #             return -2
    #
    #         # Conduct calculation if the value is 'Not assessed'
    #         elif L6_ResPrec == 'Not assessed' and \
    #                 L6_ResilPrec == 'Not assessed':
    #             return -1
    #
    #         # Conduct calculation if the value is 'No evidence'
    #         elif L6_ResPrec == 'Not evidence' and \
    #                 L6_ResilPrec == 'Not evidence':
    #             return -1
    #
    #         # Conduct calculation if the value is 'Unknown'
    #         elif L6_ResPrec == 'Unknown' and \
    #                 L6_ResilPrec == 'Unknown':
    #             return -1


    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L2' column

    def bh3_calc(row, eunislvl):
        # Run calculations for EUNIS level 2 aggregations
        # Load necessary columns into the function

        if eunislvl == 2:
            # Conduct calculation if the Resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Unassessed'
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Very low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Medium':
                return -1
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'High':
                return -1
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Not relevant':
                return -1
            elif precautionary_value(row, 'Resistance', 'L2_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L2_FinalResilience') == 'Unassessed':
                return -1


        # Run calculations for EUNIS level 3 aggregations
        elif eunislvl == 3:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Unassessed'
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Very low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Medium':
                return -1
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'High':
                return -1
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Not relevant':
                return -1
            elif precautionary_value(row, 'Resistance', 'L3_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L3_FinalResilience') == 'Unassessed':
                return -1

        # Run calculations for EUNIS level 4 aggregations
        elif eunislvl == 4:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Unassessed'
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Very low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Medium':
                return -1
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'High':
                return -1
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Not relevant':
                return -1
            elif precautionary_value(row, 'Resistance', 'L4_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L4_FinalResilience') == 'Unassessed':
                return -1

        # Run calculations for EUNIS level 5 aggregations
        elif eunislvl == 5:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Unassessed'
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Very low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Medium':
                return -1
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'High':
                return -1
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Not relevant':
                return -1
            elif precautionary_value(row, 'Resistance', 'L5_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L5_FinalResilience') == 'Unassessed':
                return -1

        # Run calculations for EUNIS level 6 aggregations
        elif eunislvl == 6:
            # Conduct calculation if the resistance value is 'None'
            if precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very low':
                return 5
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'None' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Low'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Low' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'Medium'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very low':
                return 4
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Medium' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the resistance value is 'High'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return 3
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return 2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return 2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'High' and \
                    precautionary_value(row, 'Resilience', 'L62_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Not relevant'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -2
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Not relevant' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Unassessed':
                return -1

            # Conduct calculation if the Resistance value is 'Unassessed'
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Very low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Low':
                return -1
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Medium':
                return -1
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'High':
                return -1
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Not relevant':
                return -1
            elif precautionary_value(row, 'Resistance', 'L6_FinalResistance') == 'Unassessed' and \
                    precautionary_value(row, 'Resilience', 'L6_FinalResilience') == 'Unassessed':
                return -1

    res_resil_mergeOFF['BH3_L2'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 2), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L3' column
    res_resil_mergeOFF['BH3_L3'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 3), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L4' column
    res_resil_mergeOFF['BH3_L4'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 4), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L5' column
    res_resil_mergeOFF['BH3_L5'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 5), axis=1)

    # Use lambda calculus to apply the bh3_calc function to all rows of the res_resil_merge DF and assign the
    # resulting value to the new 'BH3_L5' column
    res_resil_mergeOFF['BH3_L6'] = res_resil_mergeOFF.apply(lambda row: bh3_calc(row, 6), axis=1)

    # Fill all blank / na values with 'Cannot complete BH3 calculation'
    res_resil_mergeOFF['BH3_L2'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L3'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L4'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L5'].fillna('Cannot complete BH3 calculation', inplace=True)
    res_resil_mergeOFF['BH3_L6'].fillna('Cannot complete BH3 calculation', inplace=True)

    # Rearrange the merged DF to represent a coherent schema and structure
    res_resil_mergeOFF = res_resil_mergeOFF[[
        'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResistance', 'L2_FinalResilience',
        'BH3_L2', 'Level_3', 'L3_FinalResistance', 'L3_FinalResilience',
        'BH3_L3', 'Level_4', 'L4_FinalResistance', 'L4_FinalResilience',
        'BH3_L4', 'Level_5', 'L5_FinalResistance', 'L5_FinalResilience',
        'BH3_L5', 'Level_6', 'L6_FinalResistance', 'L6_FinalResilience',
        'BH3_L6'
    ]]

    # Export the output res_resil_mergeOFF DF with all computed BH3 values

    # Define folder file path to be saved into
    outpath = "./Output/"
    # Define file name to save, categorised by date
    filename = "BH3_OffSens_" + (time.strftime("%Y%m%d") + '_' + str(res_version) + '_'
                                                         + str(resil_version) + ".csv")
    # Run the output DF.to_csv method
    res_resil_mergeOFF.to_csv(outpath + filename, sep=',')

    ####################################################################################################################

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and
    # round value to 1 decimal place.
    print("...The 'BH3_SensitivityCalculation Offshore' script took " + str(
        round(elapsed / 60, 1)) + ' minutes to run and complete.' + '\n' +
        'These have been saved as .CSV outputs at the following filepath: ' +
        str(outpath) + '\n\n')

