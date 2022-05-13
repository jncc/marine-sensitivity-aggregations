########################################################################################################################

# Title: MCZ Wales Inshore Broadscale Habitat Sensitivity Aggregation

# Authors: Matear, L.(2020)                                                           Email: marinepressures@jncc.gov.uk
# Version Control: 1.0

# Script description:    Aggregate MarESA sensitivity assessments for Welsh inshore MCZ broadscale habitats.

#                        For any enquiries please contact marinepressures@jncc.gov.uk


###############################################################################

#                                               Aggregation Preparation:                                               #

###############################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required or data manipulation
import os
import time
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

#############################################################


# Define the code as a function to be executed as necessary
def main(marESA_file, WelshBSH):
    # Test the run time of the function
    start = time.process_time()
    print('MCZ Wales Inshore aggregation script started...')

    # Load in all BSH data from MS xlsx document
    bsh = pd.read_csv("./MarESA/Data/" + WelshBSH)

    # Import all data within the MarESA extract as Pandas DataFrame
    # NOTE: This must be updated each time a new MarESA Extract is released
    # The top copy of the MarESA Extract can be found at the following file path:
    # \\jncc-corpfile\gis\Reference\Marine\Sensitivity
    # MarESA = pd.read_excel("./Data/" + marESA_file,
    #                        marESA_tab, dtype={'EUNIS_Code': str})
    MarESA = pd.read_csv("./MarESA/Data/" + marESA_file,
                           dtype={'EUNIS_Code': str})

    def fill_missing_maresa_rows(df):

        hab_cols = ['habitatID', 'JNCC_Code', 'JNCC_Name', 'EUNIS_Code', 'EUNIS_Name', 
                    'Biological_zone', 'Zone', 'habitatInformationReviewDate', 'url']
        pressure_cols = ['NE_Code', 'Pressure']
        res_cols = ['Resistance', 'ResistanceQoE', 'ResistanceAoE', 'ResistanceDoE',
                'Resilience', 'ResilienceQoE', 'ResilienceAoE', 'ResilienceDoE',
                'Sensitivity', 'SensitivityQoE', 'SensitivityAoE', 'SensitivityDoE']
        
        # finding all the unique habitats and pressures
        df_habs = df[hab_cols].drop_duplicates()
        df_pres = df[pressure_cols].drop_duplicates()

        # creating all the possible combinations of habitat and pressure
        df_cross = df_habs.merge(df_pres, how='cross')

        # adds in the blank resistance columns 
        df_cross = df_cross.reindex(columns=hab_cols+pressure_cols+res_cols)

        # append the blank ones on the end so that drop duplicates keeps
        # the row with actual data if there is one
        df_final = df.append(df_cross)
        df_final.drop_duplicates(['habitatID', 'Pressure'], inplace=True)

        # blank rows should be filled with unknown to be picked up later
        df_final[res_cols] = df_final[res_cols].fillna('Unknown')

        return(df_final)

    MarESA = fill_missing_maresa_rows(MarESA)

    def remove_key_rows(df):
        climate = ['Global warming (Extreme)', 'Global warming (High)', 'Global warming (Middle)',
                   'Ocean Acidification (High)', 'Ocean Acidification (Middle)', 'Sea level rise (Extreme)',
                   'Sea level rise (High)', 'Sea level rise (Middle)', 'Marine heatwaves (High)', 
                   'Marine heatwaves (Middle)']
        bios = ['A5.5111', 'A5.5112', 'A5.3611']

        # There was an issues with these three biotopes being duplicated in
        # the feb 2022 run so we used this as a hot fix
        df_cut = pd.concat([df[~df['EUNIS_Code'].isin(bios)], df[~df['Pressure'].isin(climate)]])
        df_cut.drop_duplicates(inplace=True)
        return(df_cut)
    
    MarESA = remove_key_rows(MarESA)

    # Create variable with the MarESA Extract version date to be used
    # in the MarESA Aggregation output file name
    # UPDATE THIS WHEN YOU UPDATE THE MarESA INPUT DATA

    # Re-split the string to remove the .csv file extension
    maresa_date = str(marESA_file).split('.')[0]
    # Re-split the file name to only retain the date of creation
    maresa_date = str(maresa_date).split('_')[-1]
    # remove the hyphens
    maresa_date = maresa_date.replace('-', '')
    # Create an abreviated version of the filename with the date
    maresa_version = 'marESA' + maresa_date

    #############################################################

    # Clean all data and merge together

    # Strip all trailing whitespace from both the annex1 DF and the maresa DF prior to merging

    bsh['JNCC code'] = bsh['JNCC code'].str.strip()
    MarESA['JNCC_Code'] = MarESA['JNCC_Code'].str.strip()

    # Merge MarESA sensitivity assessments with all data within the bsh DF on JNCC code
    maresa_bsh_merge = pd.merge(bsh, MarESA, left_on='JNCC code', right_on='JNCC_Code', how='outer', indicator=True)

    # Create a subset of the maresa_bsh_merge which only contains the bsh without MarESA assessments
    bsh_only = maresa_bsh_merge.loc[maresa_bsh_merge['_merge'].isin(['left_only'])]

    # Create a subset of the maresa_bsh_merge which only contains the bsh with MarESA assessments
    bsh_maresa = maresa_bsh_merge.loc[maresa_bsh_merge['_merge'].isin(['both'])]

    #############################################################

    # Step 1:
    # Assign a full set of pressures with the value 'Unknown' to all bsh data which do not have MarESA Assessments

    # Identify all pressures and assign as a list
    maresa_pressures = list(MarESA['Pressure'].unique())

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

    # Create template DF
    Template_DF = PressuresCodes

    # Create function to complete cross join / create cartesian product between two target DF

    def df_crossjoin(df1, df2):
        """
        Make a cross join (cartesian product) between two dataframes by using a constant temporary key.
        Also sets a MultiIndex which is the cartesian product of the indices of the input dataframes.
        :param df1 dataframe 1
        :param df1 dataframe 2

        :return cross join of df1 and df2
        """
        df1.loc[:, '_tmpkey'] = 1
        df2.loc[:, '_tmpkey'] = 1

        res = pd.merge(df1, df2, on='_tmpkey').drop('_tmpkey', axis=1)
        res.index = pd.MultiIndex.from_product((df1.index, df2.index))

        df1.drop('_tmpkey', axis=1, inplace=True)
        df2.drop('_tmpkey', axis=1, inplace=True)

        return res

    # Perform cross join to blanket all pressures with unknown values to all EUNIS codes within the correlation_snippet
    bsh_unknown_template_cjoin = df_crossjoin(bsh_only, Template_DF)

    # Rename columns to match MarESA data
    bsh_unknown_template_cjoin.rename(
        columns={
            'Pressure_y': 'Pressure', 'Resistance_y': 'Resistance',
            'Resilience_y': 'Resilience', 'Sensitivity_y': 'Sensitivity', 'JNCC_Code_y': 'JNCC_Code'}, inplace=True)

    # Restructure the crossjoined DF to only retain columns of interest
    bsh_unknown = bsh_unknown_template_cjoin[
        ['BSH', 'Depth', 'JNCC code', 'JNCC name', 'Pressure', 'Resistance', 'Resilience', 'Sensitivity']
    ]

    # Refine the bsh_maresa dF to match the columns of the newly created bsh_unknown template
    bsh_maresa = bsh_maresa[
        ['BSH', 'Depth', 'JNCC code', 'JNCC name', 'Pressure', 'Resistance', 'Resilience', 'Sensitivity']
    ]

    # Append the bsh_unknown into the refined bsh_maresa DF
    bsh_maresa_unknowns = bsh_maresa.append(bsh_unknown, ignore_index=True)

    #############################################################

    # Step 2:
    # Prepare the data for aggregation analyses

    # Reformat contents of assessment columns within DF
    bsh_maresa_unknowns['Sensitivity'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    bsh_maresa_unknowns['Sensitivity'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    bsh_maresa_unknowns['Sensitivity'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    bsh_maresa_unknowns['Sensitivity'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

    bsh_maresa_unknowns['Resistance'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    bsh_maresa_unknowns['Resistance'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    bsh_maresa_unknowns['Resistance'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    bsh_maresa_unknowns['Resistance'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

    bsh_maresa_unknowns['Resilience'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
    bsh_maresa_unknowns['Resilience'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
    bsh_maresa_unknowns['Resilience'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
    bsh_maresa_unknowns['Resilience'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    bsh_maresa_unknowns['Sensitivity'].fillna(value='', inplace=True)

    # Create function which takes the Annex I Feature/SubFeature columns, and combines both entries into a single column
    # This enables the data to be grouped and aggregated using the .groupby() function (does not support multiple
    # simultaneous aggregations)
    def together(row):
        # Pull in data from both columns of interest
        BSH = row['BSH']
        depth = row['Depth']
        # Return a string of both individual targets combined by a ' - ' symbol
        return str(str(BSH) + ' - ' + str(depth))

    # Run the together() function to combine Feature/SubFeature data into a single 'FeatureSubFeatureDepth' column to be
    # aggregated
    bsh_maresa_unknowns['BSHDepth'] = bsh_maresa_unknowns.apply(lambda row: together(row), axis=1)

    #############################################################

    # Step 3:
    # Aggregation analyses

    # Aggregate data together by unique instances of HOCI and Bioregion
    bsh_agg = bsh_maresa_unknowns.groupby(['Pressure', 'BSHDepth'])['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    bsh_agg = pd.DataFrame(bsh_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    bsh_agg = bsh_agg.reset_index(inplace=False)

    # Define function to count the number of assessment values

    # Define all functions which are required within the script to execute aggregation process
    # Function Title: counter
    def counter(value):
        """Count the total no. of occurrences of each sensitivity (high, medium, low, not sensitive, not relevant,
          no evidence, not assessed, unknown
          Return values to be assigned to new columns through lambda function"""
        counthigh = value.count('High')
        countmedium = value.count('Medium')
        countlow = value.count('Low')
        countns = value.count('Not sensitive')
        countnr = value.count('Not relevant')
        countne = value.count('No evidence')
        countna = value.count('Not assessed')
        countuk = value.count('Unknown')
        return counthigh, countmedium, countlow, countns, countnr, countne, countna, countuk

    # Apply the counter() function to the DF to count the occurrence of all assessment values
    bsh_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = bsh_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later in code
    bsh_agg['Count_High'] = bsh_agg['High']
    bsh_agg['Count_Medium'] = bsh_agg['Medium']
    bsh_agg['Count_Low'] = bsh_agg['Low']
    bsh_agg['Count_NotSensitive'] = bsh_agg['Not sensitive']
    bsh_agg['Count_NotRel'] = bsh_agg['Not relevant']
    bsh_agg['Count_NoEvidence'] = bsh_agg['No evidence']
    bsh_agg['Count_NotAssessed'] = bsh_agg['Not assessed']
    bsh_agg['Count_Unknown'] = bsh_agg['Unknown']

    # Create colNames list for use with replacer() function
    colNames = ['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed', 'Unknown']

    # Define replacer() function to fill a numerical with the repstring being analysed
    # Function Title: replacer
    def replacer(value, repstring):
        """Perform string replace on each sensitivity count column (one set of duplicates only)"""
        if value == 0:
            return 'NA'
        elif value != 0:
            return repstring

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        bsh_agg[eachCol] = bsh_agg[eachCol].apply(lambda x: replacer(x, eachCol))

    # Function Title: final_sensitivity
    def final_sensitivity(df):
        """Create a return of a string value which gives final sensitivity score dependent on conditional statements"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
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
        if 'Not sensitive' in nsens:
            ns = 'Not sensitive'
            value.append(ns)
        if 'High' not in high and 'Medium' not in med and 'Low' not in low and 'Not sensitive' not in nsens:
            if 'Not relevant' in nrel:
                nr = 'Not relevant'
                value.append(nr)
            if 'No evidence' in nev:
                ne = 'No evidence'
                value.append(ne)
            if 'Not assessed' in n_ass:
                nass = 'Not assessed'
                value.append(nass)
        if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens and 'NA' in nrel and 'NA' in nev and \
                'NA' in n_ass:
            un = 'Unknown'
            value.append(un)

        s = ', '.join(set(value))
        return str(s)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    bsh_agg['AggregatedSensitivity'] = bsh_agg.apply(lambda df: final_sensitivity(df), axis=1)

    # Define function to calculate the total count of all assessed values
    # Function Title: combine_assessedcounts
    def combine_assessedcounts(df):
        """Conditional statements which combine assessed count data and return as string value"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        high = df['High']
        med = df['Medium']
        low = df['Low']
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
            h = 'H(' + str(df['Count_High']) + ')'
            value.append(h)
        if 'Medium' in med:
            m = 'M(' + str((df['Count_Medium'])) + ')'
            value.append(m)
        if 'Low' in low:
            lo = 'L(' + str(df['Count_Low']) + ')'
            value.append(lo)
        if 'Not sensitive' in nsens:
            ns = 'NS(' + str(df['Count_NotSensitive']) + ')'
            value.append(ns)
        if 'Not relevant' in nrel:
            nr = 'NR(' + str(df['Count_NotRel']) + ')'
            value.append(nr)
        if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens and 'NA' in nrel:
            if 'No evidence' in nev:
                ne = 'Not Applicable'
                value.append(ne)
            if 'Not assessed' in n_ass:
                nass = 'Not Applicable'
                value.append(nass)
            if 'Unknown' in un:
                unk = 'Not Applicable'
                value.append(unk)
        s = ', '.join(set(value))
        return str(s)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    bsh_agg['AssessedCount'] = bsh_agg.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Define function to calculate the total count of all unassessed values
    # Function Title: combine_unassessedcounts
    def combine_unassessedcounts(df):
        """Conditional statements which combine unassessed count data and return as string value"""
        # Create object oriented variable for each column of data from DataFrame (assessed only)
        # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
        nrel = df['Not relevant']
        nev = df['No evidence']
        n_ass = df['Not assessed']
        un = df['Unknown']

        # Create empty list for all string values to be appended into - this will be assigned to each field when data
        # are iterated through using the lambdas function which follows immediately after this function

        values = []

        # Create series of conditional statements to append string values into the empty list ('value') if conditional
        # statements are fulfilled

        if 'No evidence' in nev:
            ne = 'NE(' + str(df['Count_NoEvidence']) + ')'
            values.append(ne)
        if 'Not assessed' in n_ass:
            na = 'NA(' + str(df['Count_NotAssessed']) + ')'
            values.append(na)
        if 'Unknown' in un:
            unk = 'UN(' + str(df['Count_Unknown']) + ')'
            values.append(unk)
        # if 'NA' in nrel and 'NA' in nev and 'NA' in n_ass and 'NA' in un:
        if 'NA' in nev and 'NA' in n_ass and 'NA' in un:
            napp = 'Not Applicable'
            values.append(napp)
        s = ', '.join(set(values))
        return str(s)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    bsh_agg['UnassessedCount'] = bsh_agg.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Define function to create confidence value for the aggregation process
    # Function Title: create_confidence
    def create_confidence(df):
        """Divide the total assessed counts by the total count of all data and return as numerical value"""
        # Pull in assessed values counts
        count_high = df['Count_High']
        count_med = df['Count_Medium']
        count_low = df['Count_Low']
        count_ns = df['Count_NotSensitive']

        # Pull in unassessed values counts
        count_nr = df['Count_NotRel']
        count_ne = df['Count_NoEvidence']
        count_na = df['Count_NotAssessed']
        count_unk = df['Count_Unknown']

        # Create ratio calculation
        total_ass = count_high + count_med + count_low + count_ns
        total = total_ass + count_ne + count_na + count_unk

        return round(total_ass / total, 3) if total else 0

    # Use lambda function to apply create_confidence() function to the DataFrame
    bsh_agg['AggregationConfidenceValue'] = bsh_agg.apply(lambda df: create_confidence(df), axis=1)

    # Function Title: categorise_confidence
    def categorise_confidence(df, column):
        """Partition and categorise confidence values by quantile intervals"""
        if column == 'AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'

    # Create categories for confidence values: EUNIS Level 2
    bsh_agg['AggregationConfidenceScore'] = bsh_agg.apply(
        lambda df: categorise_confidence(df, 'AggregationConfidenceValue'), axis=1)

    ####################################################################################################################

    # Split the FeatureSubFeatureDepth column back into the individual columns once aggregated by unique combination of
    # Feature and Sub-Feature.

    def str_split(row, str_interval):
        # Import the target column into the local scope of the function
        target_col = row['BSHDepth']
        # Split the target string to get the Feature using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'BSH':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the first of the two list items
            return str(result[0])
        # Split the target string to get the Depth using ' - ', as created with the 'together()' function earlier
        # in the script
        if str_interval == 'Depth':
            # Split the string and place both halves into a list
            result = target_col.split(' - ')
            # Slice the list to return the second of the two list items
            return str(result[1])

    # Run the str_split() function to return the combined Feature data back into two separate columns
    bsh_agg['BSH'] = bsh_agg.apply(lambda row: str_split(row, 'BSH'), axis=1)

    # Run the str_split() function to return the combined Depth data back into two separate columns
    bsh_agg['Depth zone'] = bsh_agg.apply(
        lambda row: str_split(row, 'Depth'),
        axis=1)

    # Drop redundant BSHDepth column from bsh_agg DF
    bsh_agg.drop(['BSHDepth'], axis=1, inplace=True)

    #############################################################

    # Step 4:
    # Post-analyses formatting

    # Refine DF to retain columns of interest
    bsh_agg = bsh_agg[
        [
            'Pressure', 'BSH', 'Depth zone', 'AggregatedSensitivity', 'AssessedCount', 'UnassessedCount',
            'AggregationConfidenceValue', 'AggregationConfidenceScore'
        ]
    ]

    # Export DF for use

    # Define folder file path to be saved into
    outpath = "./MarESA/Output/"
    # Define file name to save, categorised by date
    filename = "MCZ_Wales_In_BSH_Sens_Agg_" + (time.strftime("%Y%m%d") + '_' + str(maresa_version) +".csv")
    # Run the output DF.to_csv method
    bsh_agg.to_csv(outpath + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print('...The ' + str(filename) + ' script took ' + str(round(elapsed / 60, 1)) + ' minutes to run and complete.'
          + '\n' + 'This has been saved as a time-stamped output at the following filepath: ' + str(outpath) + '\n\n')

