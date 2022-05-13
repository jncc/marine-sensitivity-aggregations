# Title: MCZ Wales Inshore Feature of Conservation Importance Sensitivity Aggregation

# Aggregate MarESA sensitivity assessments for Welsh inshore MCZ Features of Conservation importance.

# Email: marinepressures@jncc.gov.uk
# Authors: Matear, L.(2020), Kieran Fox (2022)
# Version Control: 2.0
                       
import time
import numpy as np
import pandas as pd

import maresa_lib as ml

pd.options.mode.chained_assignment = None  # default='warn'


def main(marESA_file, WelshFOCI):
    start = time.process_time()
    print('MCZ Wales inshore FOCI script started...')

    # Load in all HOCI data from MS xlsx document
    foci = pd.read_csv("./Data/" + WelshFOCI)
    foci = ml.clean_df(foci)

    # Import marESA extract. Top copy can be found at
    # \\jncc-corpfile\gis\Reference\Marine\Sensitivity
    MarESA = pd.read_csv("./Data/" + marESA_file, dtype={'EUNIS_Code': str})
    MarESA = ml.clean_df(MarESA)

    # Fills in the rows that have no data or are missing from the 
    # maresa extract
    MarESA = ml.fill_missing_maresa_rows(MarESA)

    #############################################################

    # Clean all data and merge together

    # Merge MarESA sensitivity assessments with all data within the foci DF on JNCC code
    maresa_foci_merge = pd.merge(foci, MarESA, left_on='JNCC code', right_on='JNCC_Code', how='outer', indicator=True)

    # Create a subset of the maresa_foci_merge which only contains the foci without MarESA assessments
    foci_only = maresa_foci_merge.loc[maresa_foci_merge['_merge'].isin(['left_only'])]

    # Create a subset of the maresa_foci_merge which only contains the foci with MarESA assessments
    foci_maresa = maresa_foci_merge.loc[maresa_foci_merge['_merge'].isin(['both'])]

    #############################################################

    # Refine the foci_maresa DF for columns of interest
    foci_maresa = foci_maresa[
        ['FOCI', 'Depth','EUNIS name', 'JNCC code', 'JNCC name',
         'Pressure', 'Resistance', 'Resilience', 'Sensitivity'] 
    ]

    #############################################################

        #############################################################

    # Step 1:
    # Assign a full set of pressures with the value 'Unknown' to all foci data which do not have MarESA Assessments

    # Create subset of all unique pressures and NE codes to be used for the append. Achieve this by filtering the MarESA
    # DataFrame to only include the unique pressures from the pressures list.
    PressuresCodes = MarESA.drop_duplicates(subset=['NE_Code', 'Pressure'], inplace=False)
    PressuresCodes = ml.fill_unknown_cols(PressuresCodes)

    # Perform cross join to blanket all pressures with unknown values to all EUNIS codes within the correlation_snippet
    foci_unknown_template_cjoin = ml.df_crossjoin(foci_only, PressuresCodes)
    
    # Rename columns to match MarESA data
    foci_unknown_template_cjoin.rename(columns={
        'Pressure_y': 'Pressure', 
        'Resistance_y': 'Resistance',
        'Resilience_y': 'Resilience', 
        'Sensitivity_y': 'Sensitivity', 
        'JNCC_Code_y': 'JNCC_Code'
        }, inplace=True)

    columns_of_interest = ['FOCI', 'Depth','EUNIS name', 'JNCC code', 'JNCC name',
                           'Pressure', 'Resistance', 'Resilience', 'Sensitivity']

    # keeping only the columns of interst of the two columns so
    # they can be appended
    foci_unknown = foci_unknown_template_cjoin[columns_of_interest]
    foci_maresa = foci_maresa[columns_of_interest]
    # Append the annex_unknown into the refined annex_maresa DF
    foci_maresa = foci_maresa.append(foci_unknown, ignore_index=True)

    # Step 2:
    # Prepare the data for aggregation analyses

    # Reformat contents of assessment columns within DF
    reformat_ev = {"Not relevant (NR)": "Not relevant",
                   "No evidence (NEv)": "No evidence",
                   "Not assessed (NA)": "Not assessed",
                   "Not Assessed (NA)": "Not assessed"}
    
    foci_maresa.replace(reformat_ev, inplace=True)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    foci_maresa['Sensitivity'].fillna(value='', inplace=True)

    #############################################################

    # Step 3:
    # Aggregation analyses

    # combine columns to give something to groupby on
    foci_maresa['unique_id'] = foci_maresa['FOCI'].astype(str) + ':'\
                               + foci_maresa['Depth'].astype(str)

    # Aggregate data together by unique instances of FOCI and Bioregion
    foci_agg = foci_maresa.groupby(['Pressure', 'unique_id'])['Sensitivity'].apply(lambda x: ', '.join(x)).reset_index(inplace=False)

    # Apply the counter() function to the DF to count the occurrence of all assessment values
    foci_agg[[
        'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown']] = foci_agg.apply(lambda df: pd.Series(ml.counter(df['Sensitivity'])), axis=1)

    # Create colNames list for use with replacer() function
    colNames = {'High': 'Count_High', 
                'Medium': 'Count_Medium', 
                'Low': 'Count_Low', 
                'Not sensitive': 'Count_NotSensitive', 
                'Not relevant': 'Count_NotRel', 
                'No evidence': 'Count_NoEvidence', 
                'Not assessed': 'Count_NotAssessed', 
                'Unknown': 'Count_Unknown'}

    # creating the blank columsn which will be replaced later on
    for key, value in colNames.items():
        foci_agg[value] = foci_agg[key]

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for key in colNames:
        foci_agg[key] = foci_agg[key].apply(lambda x: ml.replacer(x, key))

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        foci_agg[eachCol] = foci_agg[eachCol].apply(lambda x: ml.replacer(x, eachCol))

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    foci_agg['AggregatedSensitivity'] = foci_agg.apply(lambda df: ml.final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    foci_agg['AssessedCount'] = foci_agg.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    foci_agg['UnassessedCount'] = foci_agg.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    foci_agg['AggregationConfidenceValue'] = foci_agg.apply(lambda df: ml.create_confidence(df), axis=1)

    # Create categories for confidence values: EUNIS Level 2
    foci_agg['AggregationConfidenceScore'] = foci_agg.apply(
        lambda df: ml.categorise_confidence(df, 'AggregationConfidenceValue'), axis=1)

    #############################################################

    # Step 4:
    # Post-analyses formatting

    # Split the unique_id column back into the individual columns once aggregated by unique combination
    # of Feature and Sub-Feature.
    foci_agg[['Feature of Conservation Importance (FOCI)', 'Depth zone']] = foci_agg.apply(lambda row: ml.split_id2(row), axis='columns', result_type='expand')

    # Reorder columns within DF
    foci_agg = foci_agg[
        ['Pressure', 'Feature of Conservation Importance (FOCI)', 'Depth zone', 'AggregatedSensitivity',
         'AssessedCount', 'UnassessedCount', 'AggregationConfidenceValue', 'AggregationConfidenceScore']
    ]

    # Export DF for use

    # Define folder file path to be saved into
    outpath = "./Output/"
    maresa_version = ml.get_file_v(marESA_file, 'marESA')
    filename = "MCZ_Wales_In_FOCI_SensAgg_" + (time.strftime("%Y%m%d") + '_' + str(maresa_version) +".csv")
    # Run the output DF.to_csv method
    foci_agg.to_csv(outpath + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print('...The ' + str(filename) + ' script took ' + str(round(elapsed / 60, 1)) + ' minutes to run and complete.'
          + '\n' + 'This has been saved as a time-stamped output at the following filepath: ' + str(outpath) + '\n\n')

if __name__ == "__main__":

    main('habitatspressures_20220310.csv', 'Welsh_Inshore_FOCI_2022-03-16.csv')


