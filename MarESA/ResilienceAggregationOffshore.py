# Title: JNCC MarESA Resilience Aggregation (EUNIS)

# Aggregate MarESA resilience assessments for UK offshore biotopes on spatial location and
# habitat classification system. For full detail of the methods used, please see:
# https://hub.jncc.gov.uk/assets/faa8722e-865d-4d9f-ab0b-15a2eaa77db0

# Email: marinepressures@jncc.gov.uk
# Authors: Matear, L.(2019), Kieran Fox (2022)
# Version Control: 2.0

import time
import numpy as np
import pandas as pd

import maresa_lib as ml

pd.options.mode.chained_assignment = None  # default='warn'

#############################################################


# Define the code as a function to be executed as necessary
def main(marESA_file, bioregions_ext):
    # Test the run time of the function
    start = time.process_time()
    print('Offshore resilience script started...')

    # Loading all data into the IDE
    # Setting a working directory for input data set
    #os.chdir(r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Unknowns_InputData")

    # Import the JNCC Correlation Table as Pandas DataFrames - updated
    # with CorrelationTable_C16042020
    CorrelationTable = pd.read_excel("./Data/CorrelationTable_C16042020.xlsx",
        'Correlations', dtype=str)

    # Import all data within the MarESA extract as Pandas DataFrame
    #MarESA = pd.read_excel("/" + marESA_file,
    #                       marESA_tab, dtype={'EUNIS_Code': str})
    MarESA = pd.read_csv("./Data/" + marESA_file,
                           dtype={'EUNIS_Code': str})

    # Fills in the rows that have no data or are missing from the 
    # maresa extract
    MarESA = ml.fill_missing_maresa_rows(MarESA)

    # Formatting the CorrelationTable DF

    # Subset data set to only comprise values where the listed biotopes value is not recorded as False or 'nan'
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['UK Habitat'].isin(['False'])]
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['UK Habitat'].isin(['nan'])]

    # Drop any unknown values from the CorrelationTable DF
    CorrelationTable.dropna(subset=['UK Habitat'], inplace=True)

    # Subset data set to exclude any EUNIS level 1, 2 and 3 data as these do not have associated sensitivity
    # assessments
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['EUNIS level'].isin(['1', '2', '3'])]

    #############################################################

    # Formatting the MarESA DF

    # Adding a EUNIS level column to the DF based on the 'EUNIS_Code' column - using the function
    MarESA['EUNIS level'] = MarESA.apply(lambda row: ml.eunis_lvl(row), axis=1)

    # Subset data set to exclude any EUNIS level 1, 2 and 3 data as these do not have associated sensitivity
    # assessments
    MarESA = MarESA.loc[~MarESA['EUNIS level'].isin(['1', '2', '3'])]

    #############################################################

    # Identifying the unique EUNIS codes within each data set

    # Create list of all unique values within the CorrelationTable DF
    CorrelationTable_EUNIS = list(CorrelationTable['EUNIS code 2007'].unique())

    # Create list of all unique values within the MarESA DF
    MarESA_EUNIS = list(MarESA['EUNIS_Code'].unique())

    # 3.3 Create a list of all unique EUNIS codes which are present within the CorrelationTableDF, but not the MarESA DF
    EUNIS_Difference = list(set(CorrelationTable_EUNIS) - set(MarESA_EUNIS))

    # Adding the CorrelationTable DF EUNIS data not in the MarESA DF as 'Unknown' values

    # Sub-setting the CorrelationTable to only include the EUNIS codes identified within the EUNIS_Difference list
    CorrelationTable_Subset = CorrelationTable.loc[CorrelationTable['EUNIS code 2007'].isin(EUNIS_Difference)]

    # Renaming the columns within the CorrelationTable_Subset DF to match the relevant MarESA columns
    CorrelationTable_Subset.rename(columns={'EUNIS code 2007': 'EUNIS_Code', 'EUNIS name 2007': 'JNCC_Name',
                                            'JNCC 15.03 code': 'JNCC_Code'}, inplace=True)

    # Pull out list of all unique pressures
    Pressures = list(MarESA['Pressure'].unique())

    # Create subset of all unique pressures and NE codes to be used for the append. Achieve this by filtering the MarESA
    # DataFrame to only include the unique pressures from the pressures list.
    PressuresCodes = MarESA.drop_duplicates(subset=['NE_Code', 'Pressure'], inplace=False)

    # Set all values within the 'Resistance', 'ResistanceQoE', 'ResistanceAoE', 'ResistanceDoE', 'Resilience',
    # 'ResilienceQoE', 'ResilienceAoE',  'resilienceDoE',  'Sensitivity',  'SensitivityQoE',  'SensitivityAoE',
    # 'SensitivityDoE' columns to 'Unknown'

    PressuresCodes.loc[:, 'Resistance'] = 'Unknown'
    PressuresCodes.loc[:, 'ResistanceQoE'] = 'Unknown'
    PressuresCodes.loc[:, 'ResistanceAoE'] = 'Unknown'
    PressuresCodes.loc[:, 'ResistanceDoE'] = 'Unknown'
    PressuresCodes.loc[:, 'Resilience'] = 'Unknown'
    PressuresCodes.loc[:, 'ResilienceQoE'] = 'Unknown'
    PressuresCodes.loc[:, 'ResilienceAoE'] = 'Unknown'
    PressuresCodes.loc[:, 'resilienceDoE'] = 'Unknown'
    PressuresCodes.loc[:, 'Sensitivity'] = 'Unknown'
    PressuresCodes.loc[:, 'SensitivityQoE'] = 'Unknown'
    PressuresCodes.loc[:, 'SensitivityAoE'] = 'Unknown'
    PressuresCodes.loc[:, 'SensitivityDoE'] = 'Unknown'

    # Change the following values to 'Not a Number' / nan values to be filled by the fill_metadata() function
    PressuresCodes.loc[:, 'EUNIS_Code'] = np.nan
    PressuresCodes.loc[:, 'Name'] = np.nan
    PressuresCodes.loc[:, 'JNCC_Name'] = np.nan
    PressuresCodes.loc[:, 'JNCC_Code'] = np.nan
    PressuresCodes.loc[:, 'EUNIS level'] = np.nan

    # Remove name column from template before insertion

    # Create list of all unique EUNIS codes in new correlation table data
    CorrelationEUNIS_list = list(CorrelationTable['EUNIS code 2007'].unique())

    # Create template DF
    PressuresCodes = PressuresCodes

    # Create snippet of the correlation table including only the unique biotope codes which do not exist in the MarESA
    # data
    correlation_snippet = CorrelationTable_Subset.loc[CorrelationTable_Subset['EUNIS_Code'].isin(CorrelationEUNIS_list)]

    # Remove unwanted erroneous biotope codes from the correlation_snippet data
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'LS.LMp.Sm.SM16._']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'LS.LMp.Sm.SM13._']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'LS.LMp.Sm_']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'SS.SCS.SCSVS']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'Saltmarsh 5 EUNIS types Sm:']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code !=
                                              '104 EUNIS level 5 and 6 types 26 NVC types:']


    # Perform cross join to blanket all pressures with unknown values
    # to all EUNIS codes within the correlation_snippet
    correlation_snippet_template = ml.df_crossjoin(correlation_snippet,
                                                PressuresCodes)

    # Drop unwanted columns from the correlation_snippet_template data
    correlation_snippet_template.drop(['JNCC_Code_y', 'JNCC_Name_y',
        'EUNIS_Code_y', 'EUNIS level_y'], axis=1, inplace=True)

    # Rename columns to match MarESA data
    correlation_snippet_template.rename(columns={
        'EUNIS_Code_x': 'EUNIS_Code',
        'EUNIS level_x': 'EUNIS level',
        'JNCC_Code_x': 'JNCC_Code',
        'JNCC_Name_x': 'JNCC_Name'}, inplace=True)

    # Order columns to match MarESA data
    correlation_snippet_template = correlation_snippet_template[[
        'JNCC_Code', 'JNCC_Name', 'EUNIS_Code', 'Name',
        'NE_Code', 'Pressure', 'Resistance', 'ResistanceQoE',
        'ResistanceAoE', 'ResistanceDoE', 'Resilience',
        'ResilienceQoE', 'ResilienceAoE', 'resilienceDoE',
        'Sensitivity', 'SensitivityQoE', 'SensitivityAoE',
        'SensitivityDoE', 'url', 'EUNIS level'
        ]]

    # Append the correlation_snippet_template into the MarESA data to
    # have a MarESA dataset which accounts for 'unknown' values.
    maresa = MarESA.append(correlation_snippet_template, ignore_index=True)

    ####################################################################
    #
    #                  B. MarESA Aggregation Process
    #
    ####################################################################

    bioregions = pd.read_excel('./Data/' + bioregions_ext, dtype=str)

    # Develop a subset of the bioregions data which only contains EUNIS codes of string length 4 or greater
    # This will remove any unwanted EUNIS L1 - L3 from the data
    bioregions = bioregions[bioregions['HabitatCode'].astype(str).map(len) >= 5]

    # Following contact between Pressures & Impacts / Mapping Team staff, the biotopes A6.95 and A6.9111 were identified
    # to be erroneous. Therefore, this data are required to be removed from the input data.
    maresa = maresa[maresa.EUNIS_Code != 'A6.95']
    maresa = maresa[maresa.EUNIS_Code != 'A6.9111']
    bioregions = bioregions[bioregions.HabitatCode != 'A6.95']
    bioregions = bioregions[bioregions.HabitatCode != 'A6.9111']

    ####################################################################################################################

    # Defining functions (aggregation process data formatting)

    # Define all functions which are required within the script to format data for aggregation process

    # Function Title: df_clean
    def df_clean(df):
        """User defined function to refine dataset - remove whitespace
         and Inshore / No Biotope Presence data"""
        # Toggle this on / off to get offshore or all data together
        df.drop(df[df.BiotopePresence == 'Inshore only'].index, inplace=True)
        # Refine dataset to only include data for which BiotopePresence == 'Poss' or 'Yes'
        df.drop(df[df.BiotopePresence == 'No'].index, inplace=True)
        df['EUNIS_Code'].str.strip()
        df['Resilience'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
        df['Resilience'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
        df['Resilience'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
        df['Resistance'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

        return df

    # Function Title: unwanted_char
    def unwanted_char(df, column):
        """User defined function to refine dataset - remove unwanted special characters and acronyms from resilience
        scores. User must pass the DataFrame and the column (as a string) as the arguments to the parentheses of the
        function"""
        for eachField in df[column]:
            eachField.replace(r"\\s*zz(][^\\]+\\)", "")
        return df

    # Function Title: eunis_col
    def eunis_col(row):
        """User defined function to pull out all entries in EUNIS_Code column and create returns based on string
        slices of the EUNIS data. This must be used with df.apply() and a lambda function.

        e.g. fact_tbl[['Level_1', 'Level_2', 'Level_3',
              'Level_4', 'Level_5', 'Level_6']] = fact_tbl.apply(lambda row: pd.Series(eunis_col(row)), axis=1)"""

        # Create object oriented variable to store EUNIS_Code data
        ecode = str(row['EUNIS_Code'])
        # Create if / elif conditions to produce response dependent on the string length of the inputted data.
        if len(ecode) == 1:
            return ecode[0:1], None, None, None, None, None
        elif len(ecode) == 2:
            return ecode[0:1], ecode[0:2], None, None, None, None
        elif len(ecode) == 4:
            return ecode[0:1], ecode[0:2], ecode[0:4], None, None, None
        elif len(ecode) == 5:
            return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], None, None
        elif len(ecode) == 6:
            return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], None
        elif len(ecode) == 7:
            return ecode[0:1], ecode[0:2], ecode[0:4], ecode[0:5], ecode[0:6], ecode[0:7]

    ####################################################################################################################

    # Defining functions (aggregation process)

    # Function Title: create_resilience
    def create_resilience(df):
        """Series of conditional statements which return a string value of all assessment values
        contained within individual columns"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
        vlow = df['Very low']
        nsens = df['Not sensitive']
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function
        value = []
        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled
        if 'High' in high:
            h = 'High'
            value.append(h)
        if 'Medium' in med:
            m = 'Medium'
            value.append(m)
        if 'Low' in low:
            lo = 'Low'
            value.append(lo)
        if 'Very low' in vlow:
            vlo = 'Very low'
            value.append(vlo)
        if 'Not sensitive' in nsens:
            ns = 'Not sensitive'
            value.append(ns)
        if 'Not relevant' in nrel:
            nr = 'Not relevant'
            value.append(nr)
        if 'No evidence' in nev:
            ne = 'No evidence'
            value.append(ne)
        if 'Not assessed' in n_ass:
            nass = 'Not assessed'
            value.append(nass)
        if 'Unknown' in un:
            unk = 'Unknown'
            value.append(unk)
        s = ', '.join(value)
        return str(s)

    # Function Title: final_resilience
    def final_resilience(df):
        """Create a return of a string value which gives final resilience score dependent on conditional statements"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
        vlow = df['Very low']
        nsens = df['Not sensitive']
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function
        value = []
        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled
        if 'High' in high:
            h = 'High'
            value.append(h)
        if 'Medium' in med:
            m = 'Medium'
            value.append(m)
        if 'Low' in low:
            lo = 'Low'
            value.append(lo)
        if 'Very low' in vlow:
            vlo = 'Very low'
            value.append(vlo)
        if 'Not sensitive' in nsens:
            ns = 'Not sensitive'
            value.append(ns)
        if 'High' not in high and 'Medium' not in med and 'Low' not in low and 'Very low' not in vlow and \
                'Not sensitive' not in nsens:
            if 'Not relevant' in nrel:
                nr = 'Not relevant'
                value.append(nr)
            if 'No evidence' in nev:
                ne = 'No evidence'
                value.append(ne)
            if 'Not assessed' in n_ass:
                nass = 'Not assessed'
                value.append(nass)
            if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in vlow and 'NA' in nsens and 'NA' in nrel and \
                    'NA' in nev and 'NA' in n_ass:
                if 'Unknown' in un:
                    unk = 'Unknown'
                    value.append(unk)

        s = ', '.join(value)
        return str(s)

    # Function Title: column5
    def column5(df, column):
        """Sample Level_5 column and return string variables sliced within the range [0:6]"""
        value = df[column]
        sample = value[0:6]
        return sample

    # Function Title: column4
    def column4(df, column):
        """Sample Level_5 column and return string variables sliced within the range [0:5]"""
        value = df[column]
        sample = value[0:5]
        return sample

    # Function Title: column3
    def column3(df, column):
        """User defined function to sample Level_4 column and return string variables sliced within the range [0:4]"""
        value = df[column]
        sample = value[0:4]
        return sample

    # Function Title: column2
    def column2(df, column):
        """User defined function to sample Level_4 column and return string variables sliced within the range [0:4]"""
        value = df[column]
        sample = value[0:2]
        return sample

    ####################################################################################################################

    # Data formatting

    # Remove unnecessary whitespace, rename Bioregions column and merge data sets

    # Rename bioregions column to facilitate merge
    bioregions.rename(columns={'HabitatCode': 'EUNIS_Code'}, inplace=True)

    # bioreg_maresa_merge = pd.merge(bioregions, maresa, on='EUNIS_Code', how='outer', indicator=True)
    bioreg_maresa_merge = pd.merge(bioregions, maresa, on='EUNIS_Code')

    # Refine the DF to remove the currently not needed Region 8 (deep dea)
    bioreg_maresa_merge = bioreg_maresa_merge[bioreg_maresa_merge['SubregionName'] != 'Region 8 (deep-sea)']

    # Remove unwanted data by passing the bioreg_maresa_merge DF to the df_clean() function
    df_clean(bioreg_maresa_merge)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    bioreg_maresa_merge['Resilience'].fillna(value='', inplace=True)

    # Remove unwanted special characters from data by passing the bioreg_maresa_merge to the unwanted_char() function
    bioreg_maresa_merge = unwanted_char(bioreg_maresa_merge, 'Resilience')

    # Create individual EUNIS level columns in bioreg_maresa_merge using a lambda function and apply() method on
    # 'EUNIS_Code' column on DF.
    bioreg_maresa_merge[[
        'Level_1', 'Level_2', 'Level_3', 'Level_4', 'Level_5', 'Level_6'
    ]] = bioreg_maresa_merge.apply(lambda row: pd.Series(eunis_col(row)), axis=1)

    # Create new 'EUNIS_Level' column which indicates the numerical value of the EUNIS level by passing the bioreg_
    # maresa_merge to the eunis_lvl() function.
    bioreg_maresa_merge['EUNIS_Level'] = bioreg_maresa_merge.apply(lambda row: ml.eunis_lvl(row), axis=1)

    # De-capitalise the low in all 'Very Low' Resilience scores to enable for differentiation between Low and vLow
    # analyses within the script
    bioreg_maresa_merge.loc[bioreg_maresa_merge['Resilience'] == 'Very Low', 'Resilience'] = 'Very low'

    ####################################################################################################################

    # Level 6 data only

    # Extract all level 6 data only and assign to object oriented variable to be aggregated to level 5
    original_L6_data = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['EUNIS_Level'].isin(['6'])])

    # Group all L6 data by Resilience values
    L6_processed = original_L6_data.groupby(['Level_6', 'Pressure', 'SubregionName'
                                             ])['Resilience'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L6_processed = pd.DataFrame(L6_processed)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L6_processed = L6_processed.reset_index(inplace=False)

    # Reset columns within L6_processed DataFrame
    L6_processed.columns = ['Level_6', 'Pressure', 'SubregionName', 'Resilience']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L6_processed[
        [
            'High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown'
        ]
    ] = L6_processed.apply(lambda df: pd.Series(ml.counter_rl(df['Resilience'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L6_processed['Count_High'] = L6_processed['High']
    L6_processed['Count_Medium'] = L6_processed['Medium']
    L6_processed['Count_Low'] = L6_processed['Low']
    L6_processed['Count_vLow'] = L6_processed['Very low']
    L6_processed['Count_NotSensitive'] = L6_processed['Not sensitive']
    L6_processed['Count_NotRel'] = L6_processed['Not relevant']
    L6_processed['Count_NoEvidence'] = L6_processed['No evidence']
    L6_processed['Count_NotAssessed'] = L6_processed['Not assessed']
    L6_processed['Count_Unknown'] = L6_processed['Unknown']

    # Create colNames list for use with replacer() function
    colNames = [
        'High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed', 'Unknown'
    ]

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L6_processed[eachCol] = L6_processed[eachCol].apply(lambda x: ml.replacer(x, eachCol))

    ####################################################################################################################

    # Level 6 (aggregation)

    # Use lambda function to apply create_resilience() function to each row within the DataFrame
    L6_processed['L6_Resilience'] = L6_processed.apply(lambda df: create_resilience(df), axis=1)

    # Use lambda function to apply final_resilience() function to each row within the DataFrame
    L6_processed['L6_FinalResilience'] = L6_processed.apply(lambda df: final_resilience(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L6_processed['L6_AssessedCount'] = L6_processed.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L6_processed['L6_UnassessedCount'] = L6_processed.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Apply column5() function to L6_processed DataFrame to create new Level_5 column
    L6_processed['Level_5'] = L6_processed.apply(lambda df: pd.Series(column5(df, 'Level_6')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L6_processed['L6_AggregationConfidenceValue'] = L6_processed.apply(lambda df: ml.create_confidence(df), axis=1)

    # Format columns into correct order
    L6_processed = L6_processed[
        ['Level_5', 'Pressure', 'SubregionName', 'Level_6', 'Resilience', 'L6_Resilience',
         'L6_FinalResilience', 'L6_AssessedCount', 'L6_UnassessedCount', 'L6_AggregationConfidenceValue']
    ]

    # Remove child biotopes A5.7111 + A5.7112 from aggregation data due to prioritisation of Level 4 assessments.
    L6_processed = L6_processed[L6_processed['Level_6'] != 'A5.7111']
    L6_processed = L6_processed[L6_processed['Level_6'] != 'A5.7112']

    ####################################################################################################################

    # Level 4 to 3 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L6_export = L6_processed

    # Drop unwanted columns from L3_export DataFrame
    L6_export = L6_export.drop(['L6_Resilience', 'Resilience'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 6 to 5 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 6 to level 5

    # Group data by Level_5, Pressure, SubregionName and apply resilience values to list using lambdas function and
    # .apply() method
    aggregated_L6_to_L5 = L6_processed.groupby(['Level_5', 'Pressure', 'SubregionName']
                                               )['Resilience'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    aggregated_L6_to_L5 = pd.DataFrame(aggregated_L6_to_L5)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    aggregated_L6_to_L5 = aggregated_L6_to_L5.reset_index(inplace=False)

    # Reset columns within aggregated_L6_to_L5 DF
    aggregated_L6_to_L5.columns = ['Level_5', 'Pressure', 'SubregionName', 'Resilience']

    # Extract all original level 5 data and assign to object oriented variable
    original_L5_data = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['EUNIS_Level'].isin(['5'])])

    # Assign data differences to new object oriented variable using outer merge between data frames
    assessed_L5L6_merge = pd.merge(original_L6_data, original_L5_data, how='outer', on=['Level_5', 'Pressure'],
                                   indicator=True)

    # Create object for L5 where there is no L6 - required for new edits added 27/02/2020
    assessed_L5_without_L6 = pd.DataFrame(assessed_L5L6_merge[assessed_L5L6_merge['_merge'] == 'right_only'])

    # Refine to create a list of the unique biotopes which do not have any child L6 data
    assessed_L5_without_L6 = assessed_L5_without_L6['EUNIS_Code_y'].unique()

    # Subset  L5 original data to only retain data where there is no L6 child biotope
    # Create a list of the original L5 data which do not have associated child biotopes
    original_L5_without_L6_DF = original_L5_data.loc[original_L5_data['EUNIS_Code'].isin(assessed_L5_without_L6)]

    # Drop unwanted columns from 'original_L5_without_L6_DF' DataFrame
    original_L5_without_L6_DF = original_L5_without_L6_DF[['Level_5', 'Pressure', 'SubregionName', 'Resilience']]

    # APPEND TOGETHER aggregated_L6_to_L5 + original_L5_without_L6_DF
    aggregated_L6_to_L5 = aggregated_L6_to_L5.append(original_L5_without_L6_DF)

    # Fixing issues when there are the same pressures/subregions in the new additional data
    # to the existing data
    aggregated_L6_to_L5 = aggregated_L6_to_L5.drop_duplicates(['Level_5', 'Pressure', 'SubregionName'])

    # Apply the counter() function to the DF to count the occurrence of all assessment values
    aggregated_L6_to_L5[
        [
            'High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown'
        ]
    ] = aggregated_L6_to_L5.apply(lambda df: pd.Series(ml.counter_rl(df['Resilience'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    aggregated_L6_to_L5['Count_High'] = aggregated_L6_to_L5['High']
    aggregated_L6_to_L5['Count_Medium'] = aggregated_L6_to_L5['Medium']
    aggregated_L6_to_L5['Count_Low'] = aggregated_L6_to_L5['Low']
    aggregated_L6_to_L5['Count_vLow'] = aggregated_L6_to_L5['Very low']
    aggregated_L6_to_L5['Count_NotSensitive'] = aggregated_L6_to_L5['Not sensitive']
    aggregated_L6_to_L5['Count_NotRel'] = aggregated_L6_to_L5['Not relevant']
    aggregated_L6_to_L5['Count_NoEvidence'] = aggregated_L6_to_L5['No evidence']
    aggregated_L6_to_L5['Count_NotAssessed'] = aggregated_L6_to_L5['Not assessed']
    aggregated_L6_to_L5['Count_Unknown'] = aggregated_L6_to_L5['Unknown']

    # Level 6 to 5 subsetting data where there are Level 5 assessments without L6 assessed values

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        aggregated_L6_to_L5[eachCol] = \
            aggregated_L6_to_L5[eachCol].apply(lambda x: ml.replacer(x, eachCol))

    ####################################################################################################################

    # Level 6 to 5 aggregation (aggregation)

    # Use lambda function to apply create_resilience() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_Resilience'] = \
        aggregated_L6_to_L5.apply(lambda df: create_resilience(df), axis=1)

    # Use lambda function to apply final_resilience() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_FinalResilience'] = \
        aggregated_L6_to_L5.apply(lambda df: final_resilience(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_AssessedCount'] = \
        aggregated_L6_to_L5.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_UnassessedCount'] = \
        aggregated_L6_to_L5.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Apply column5() function to L6_processed DataFrame to create new Level_5 column
    aggregated_L6_to_L5['Level_4'] = aggregated_L6_to_L5.apply(lambda df: pd.Series(column4(df, 'Level_5')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    aggregated_L6_to_L5['L5_AggregationConfidenceValue'] = \
        aggregated_L6_to_L5.apply(lambda df: ml.create_confidence(df), axis=1)

    # Format columns into correct order
    L5_all = aggregated_L6_to_L5[
        ['Level_4', 'Pressure', 'SubregionName', 'Level_5', 'Resilience', 'L5_Resilience',
         'L5_FinalResilience', 'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AggregationConfidenceValue']
    ]

    # Create edited subset of the L5_all DF to remove 'A5.71' from any aggregation data - this will not be removed for
    # EUNIS level 4 assessments (A5.71) which have been completed.
    # L5_all = L5_all[L5_all['Level_4'] != 'A5.71']
    # Remove child biotopes A5.711 + A5.712 from aggregation data due to prioritisation of Level 4 assessments.
    L5_all = L5_all[L5_all['Level_5'] != 'A5.711']
    # L5_all = L5_all[L5_all['Level_5'] != 'A5.712']
    L5_all = L5_all[L5_all['Level_5'] != 'A5.713']
    L5_all = L5_all[L5_all['Level_5'] != 'A5.714']
    L5_all = L5_all[L5_all['Level_5'] != 'A5.715']
    L5_all = L5_all[L5_all['Level_5'] != 'A5.716']

    ####################################################################################################################

    # Create an export object for the aggregated data to be combined into the MasterFrame at the end of the script

    # Create DataFrame for master DataFrame at end of script
    L5_export = L5_all

    # Drop unwanted columns from L5_export DataFrame
    L5_export = L5_export.drop(['Resilience'], axis=1, inplace=False)

    # Rename and reorder columns within L5_export
    L5_export = L5_export[['Level_4', 'Pressure', 'SubregionName', 'Level_5', 'L5_FinalResilience',
                           'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AggregationConfidenceValue']]

    ####################################################################################################################

    # Level 5 to 4 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 5 to level 4

    # Group data by Level_4, Pressure, SubregionName and apply resilience values to list using lambdas function and
    # .apply() method
    L4_agg = L5_all.groupby(['Level_4', 'Pressure', 'SubregionName'
                             ])['Resilience'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L4_agg = pd.DataFrame(L4_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L4_agg = L4_agg.reset_index(inplace=False)

    # Reset columns within L4_agg DataFrame
    L4_agg.columns = ['Level_4', 'Pressure', 'SubregionName', 'Resilience']

    ###########################
    # ADDING IN L4 DATA WHICH IS NOT SAMPLED FROM THE L6-L5 Aggregation
    ###########################

    # Subset EUNIS L4 data from the fact_tbl - maresa / bioregions full join
    L4_maresa_insert = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['EUNIS_Level'].isin(['4'])])

    # Refine L4_maresa_insert to only include the desired columns to facilitate an append into the L4_agg DF
    L4_maresa_insert = L4_maresa_insert[['EUNIS_Code', 'Pressure', 'SubregionName', 'Resilience']]

    # Rename the columns within the L4_bio_insert DF to match those within the L4_agg DF
    L4_maresa_insert.columns = ['Level_4', 'Pressure', 'SubregionName', 'Resilience']

    # Subset the L4_maresa_insert to only include the Level 4 data which was not aggregated from 6 to 5 to 4
    aggregated_to_L4 = list(L4_agg['Level_4'].unique())

    # Subset by the L4_maresa_insert data which does not also appear within the L4 values created from 6 to 5
    # aggregations (L4_agg)
    L4_maresa_insert_without_aggregation = L4_maresa_insert.loc[~L4_maresa_insert['Level_4'].isin(aggregated_to_L4)]

    # Append the data back into the L4_agg DF
    L4_agg = L4_agg.append(L4_maresa_insert_without_aggregation)

    ###########################

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L4_agg[
        [
            'High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown'
        ]
    ] = L4_agg.apply(lambda df: pd.Series(ml.counter_rl(df['Resilience'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L4_agg['Count_High'] = L4_agg['High']
    L4_agg['Count_Medium'] = L4_agg['Medium']
    L4_agg['Count_Low'] = L4_agg['Low']
    L4_agg['Count_vLow'] = L4_agg['Very low']
    L4_agg['Count_NotSensitive'] = L4_agg['Not sensitive']
    L4_agg['Count_NotRel'] = L4_agg['Not relevant']
    L4_agg['Count_NoEvidence'] = L4_agg['No evidence']
    L4_agg['Count_NotAssessed'] = L4_agg['Not assessed']
    L4_agg['Count_Unknown'] = L4_agg['Unknown']

    # Reassign L4_agg DataFrame to L4_res for resilience aggregation
    L4_res = L4_agg

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L4_res[eachCol] = L4_res[eachCol].apply(lambda x: ml.replacer(x, eachCol))

    ####################################################################################################################

    # Level 5 to 4 aggregation (aggregation)

    # Use lambda function to apply create_resilience() function to each row within the DataFrame
    L4_res['L4_Resilience'] = L4_res.apply(lambda df: create_resilience(df), axis=1)

    # Use lambda function to apply final_resilience() function to each row within the DataFrame
    L4_res['L4_FinalResilience'] = L4_res.apply(lambda df: final_resilience(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L4_res['L4_AssessedCount'] = L4_res.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L4_res['L4_UnassessedCount'] = L4_res.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Apply column3() function to L4_res DataFrame to create new Level_3 column
    L4_res['Level_3'] = L4_res.apply(lambda df: pd.Series(column3(df, 'Level_4')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L4_res['L4_AggregationConfidenceValue'] = L4_res.apply(lambda df: ml.create_confidence(df), axis=1)

    # Format columns into correct order
    L4_res = L4_res[['Level_3', 'Pressure', 'SubregionName', 'Level_4', 'Resilience', 'L4_Resilience',
                     'L4_FinalResilience', 'L4_AssessedCount', 'L4_UnassessedCount', 'L4_AggregationConfidenceValue']]

    ####################################################################################################################

    # Level 5 to 4 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L4_export = L4_res

    # Drop unwanted columns from L4_export DataFrame
    L4_export = L4_export.drop(['Resilience', 'L4_Resilience'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 4 to 3 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 4 to level 3

    # Group data by Level_3, Pressure, SubregionName and apply resilience values to list using lambdas function and
    # .apply() method
    L3_agg = L4_res.groupby(['Level_3', 'Pressure', 'SubregionName'])['Resilience'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L3_agg = pd.DataFrame(L3_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L3_agg = L3_agg.reset_index(inplace=False)

    # Reset columns within L3_agg DataFrame
    L3_agg.columns = ['Level_3', 'Pressure', 'SubregionName', 'Resilience']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L3_agg[['High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = L3_agg.apply(lambda df: pd.Series(ml.counter_rl(df['Resilience'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L3_agg['Count_High'] = L3_agg['High']
    L3_agg['Count_Medium'] = L3_agg['Medium']
    L3_agg['Count_Low'] = L3_agg['Low']
    L3_agg['Count_vLow'] = L3_agg['Very low']
    L3_agg['Count_NotSensitive'] = L3_agg['Not sensitive']
    L3_agg['Count_NotRel'] = L3_agg['Not relevant']
    L3_agg['Count_NoEvidence'] = L3_agg['No evidence']
    L3_agg['Count_NotAssessed'] = L3_agg['Not assessed']
    L3_agg['Count_Unknown'] = L3_agg['Unknown']

    # Reassign L3_agg DataFrame to L3_res for resilience aggregation
    L3_res = L3_agg

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L3_res[eachCol] = L3_res[eachCol].apply(lambda x: ml.replacer(x, eachCol))

    ####################################################################################################################

    # Level 4 to 3 aggregation (aggregation)

    # Use lambda function to apply create_resilience() function to each row within the DataFrame
    L3_res['L3_Resilience'] = L3_res.apply(lambda df: create_resilience(df), axis=1)

    # Use lambda function to apply final_resilience() function to each row within the DataFrame
    L3_res['L3_FinalResilience'] = L3_res.apply(lambda df: final_resilience(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L3_res['L3_AssessedCount'] = L3_res.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L3_res['L3_UnassessedCount'] = L3_res.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Apply column2() function to L3_res DataFrame to create new Level_2 column
    L3_res['Level_2'] = L3_res.apply(lambda df: pd.Series(column2(df, 'Level_3')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L3_res['L3_AggregationConfidenceValue'] = L3_res.apply(lambda df: ml.create_confidence(df), axis=1)

    # Drop unwanted data from L3_res DataFrame
    L3_res = L3_res.drop([
        'Unknown', 'High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 4 to 3 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L3_export = L3_res

    # Drop unwanted columns from L3_export DataFrame
    L3_export = L3_export.drop(['L3_Resilience', 'Resilience'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 3 to 2 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 3 to level 2

    # Group data by Level_2, Pressure, SubregionName and apply resilience values to list using lambdas function and
    # .apply() method
    L2_agg = L3_res.groupby(['Level_2', 'Pressure', 'SubregionName'
                             ])['Resilience'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L2_agg = pd.DataFrame(L2_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L2_agg = L2_agg.reset_index(inplace=False)

    # Reset columns within L2_agg DataFrame
    L2_agg.columns = ['Level_2', 'Pressure', 'SubregionName', 'Resilience']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L2_agg[['High', 'Medium', 'Low', 'Very low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = L2_agg.apply(lambda df: pd.Series(ml.counter_rl(df['Resilience'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L2_agg['Count_High'] = L2_agg['High']
    L2_agg['Count_Medium'] = L2_agg['Medium']
    L2_agg['Count_Low'] = L2_agg['Low']
    L2_agg['Count_vLow'] = L2_agg['Very low']
    L2_agg['Count_NotSensitive'] = L2_agg['Not sensitive']
    L2_agg['Count_NotRel'] = L2_agg['Not relevant']
    L2_agg['Count_NoEvidence'] = L2_agg['No evidence']
    L2_agg['Count_NotAssessed'] = L2_agg['Not assessed']
    L2_agg['Count_Unknown'] = L2_agg['Unknown']

    # Reassign L2_agg DataFrame to L2_res for resilience aggregation
    L2_res = L2_agg

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L2_res[eachCol] = L2_res[eachCol].apply(lambda x: ml.replacer(x, eachCol))

    ####################################################################################################################

    # Level 3 to 2 aggregation (aggregation)

    # Use lambda function to apply create_resilience() function to each row within the DataFrame
    L2_res['L2_Resilience'] = L2_res.apply(lambda df: create_resilience(df), axis=1)

    # Use lambda function to apply final_resilience() function to each row within the DataFrame
    L2_res['L2_FinalResilience'] = L2_res.apply(lambda df: final_resilience(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L2_res['L2_AssessedCount'] = L2_res.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L2_res['L2_UnassessedCount'] = L2_res.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L2_res['L2_AggregationConfidenceValue'] = L2_res.apply(lambda df: ml.create_confidence(df), axis=1)

    # Drop unwanted data from L2_res DataFrame
    L2_res = L2_res.drop([
        'Resilience', 'Unknown', 'High', 'Medium', 'Low', 'Very low','Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    # Format columns into correct order
    L2_res = L2_res[['Level_2', 'Pressure', 'SubregionName', 'L2_FinalResilience', 'L2_Resilience',
                       'L2_AssessedCount', 'L2_UnassessedCount', 'L2_AggregationConfidenceValue']]

    ####################################################################################################################

    # Level 3 to 2 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L2_export = L2_res

    # Drop unwanted columns from L2_export DataFrame
    L2_export = L2_export.drop(['L2_Resilience'], axis=1, inplace=False)

    ####################################################################################################################

    # Creating a MasterFrame

    # Combine all exported DataFrames into one MasterFrame for export of aggregation work

    # Merge EUNIS Levels 2 and 3
    L2L3 = pd.merge(L2_export, L3_export)

    # Merge EUNIS Levels 3 and 4
    L3L4 = pd.merge(L2L3, L4_export, how='outer')

    # Merge EUNIS Levels 4 and 5
    L4L5 = pd.merge(L3L4, L5_export, how='outer')

    # Merge EUNIS Levels 4 and 5
    MasterFrame = pd.merge(L4L5, L6_export, how='outer')

    ####################################################################################################################

    # Categorising confidence values

    # Assess all confidence scores using the categorise_confidence() function developed and store
    # information in a correlating Confidence Category column.

    # Create categories for confidence values: EUNIS Level 5
    MasterFrame['L6_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: ml.categorise_confidence(df, 'L6_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 5
    MasterFrame['L5_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: ml.categorise_confidence(df, 'L5_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 4
    MasterFrame['L4_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: ml.categorise_confidence(df, 'L4_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 3
    MasterFrame['L3_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: ml.categorise_confidence(df, 'L3_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 2
    MasterFrame['L2_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: ml.categorise_confidence(df, 'L2_AggregationConfidenceValue'), axis=1)

    ####################################################################################################################

    # Exporting the MasterFrame

    # Create correct order for columns within MasterFrame
    MasterFrame = MasterFrame[
        [
            'Pressure', 'SubregionName', 'Level_2', 'L2_FinalResilience', 'L2_AssessedCount',
            'L2_UnassessedCount', 'L2_AggregationConfidenceValue', 'L2_AggregationConfidenceScore', 'Level_3',
            'L3_FinalResilience', 'L3_AssessedCount', 'L3_UnassessedCount', 'L3_AggregationConfidenceValue',
            'L3_AggregationConfidenceScore', 'Level_4', 'L4_FinalResilience', 'L4_AssessedCount',
            'L4_UnassessedCount', 'L4_AggregationConfidenceValue', 'L4_AggregationConfidenceScore', 'Level_5',
            'L5_FinalResilience', 'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AggregationConfidenceValue',
            'L5_AggregationConfidenceScore', 'Level_6', 'L6_FinalResilience', 'L6_AssessedCount', 'L6_UnassessedCount',
            'L6_AggregationConfidenceValue', 'L6_AggregationConfidenceScore'
        ]
    ]

    # All 'A2.611' values within the Level_5 column were found to be erroneous and need to be replaced with the string
    # value of 'Not Applicable'
    MasterFrame.loc[MasterFrame['Level_5'] == 'A2.611', 'L5_FinalResilience'] = 'Not Applicable'

    # All 'B3' values within the Level_2 column were found to be erroneous and need to be replaced with the string value
    # of 'Not Applicable'
    MasterFrame.loc[MasterFrame['Level_2'] == 'B3', 'L2_FinalResilience'] = 'Not Applicable'

    # Remove all A6 biotopes from the MasterFrame (temporary fix 01/07/2020)
    MasterFrame = MasterFrame[MasterFrame.Level_2 != 'A6']

    # Review the newly developed MasterFrame, and export to a .csv format file. To export the data, utilise the export
    # code which is stored as a comment (#) - ensure that you select an appropriate file path when completing this stage.

    # Export MasterFrame in CSV format  - Offshore Only

    # Define folder file path to be saved into
    outpath = "./Output/"
    bioreg_version = ml.get_file_v(bioregions_ext, 'Bioreg')
    maresa_version = ml.get_file_v(marESA_file, 'marESA')
    filename = "OffshoreResilAgg_" + (time.strftime("%Y%m%d") + "_" + str(bioreg_version) + '_' + str(maresa_version) +
                                      ".csv")
    # Run the output DF.to_csv method
    MasterFrame.to_csv(outpath + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took
    # and round value to 1 decimal place.
    print('...The ' + str(filename) + ' script took ' +
        str(round(elapsed / 60, 1)) + ' minutes to run and complete.' +
        '\n' + 'This has been saved as a time-stamped output at ' +
        'the following filepath: ' + str(outpath) + '\n\n')


    return(filename)
