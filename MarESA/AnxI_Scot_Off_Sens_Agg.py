# Title: Annex I Scotland Offshore Sensitivity Aggregation

# Aggregate MarESA sensitivity assessments for offshore Habitats Directive Annex I Features.

# Email: marinepressures@jncc.gov.uk
# Authors: Matear, L.(2020)  & Robson, L. (2021), Kieran Fox (2022)
# Version Control: 2.0

import time
import numpy as np
import pandas as pd

import maresa_lib as ml

pd.options.mode.chained_assignment = None  # default='warn'


def main(marESA_file, Scot_Annex1):
    start = time.process_time()
    print('starting the anxI Scotland off sensitivity script...')

    # Load all Annex 1 sub-type data into Pandas DF from MS Office .xlsx docuent
    annex1 = pd.read_csv("./Data/" + Scot_Annex1)
    annex1 = ml.clean_df(annex1)

    # Import marESA extract. Top copy can be found at
    # \\jncc-corpfile\gis\Reference\Marine\Sensitivity
    MarESA = pd.read_csv("./Data/" + marESA_file, dtype={'EUNIS_Code': str})
    MarESA = ml.clean_df(MarESA)

    # Fills in the rows that have no data or are missing from the 
    # maresa extract
    MarESA = ml.fill_missing_maresa_rows(MarESA)

    #############################################################

    # Merge MarESA sensitivity assessments with all Habitats Directive listed Annex 1 habitats and sub-types of
    # relevance
    # Merge MarESA sensitivity assessments with all data within the annex DF on JNCC code
    maresa_annex_merge = pd.merge(annex1, MarESA, left_on='EUNIS code', right_on='EUNIS_Code',
                                  how='outer', indicator=True)

    # Create a subset of the maresa_annex_merge which only contains the annex without MarESA assessments
    annex_only = maresa_annex_merge.loc[maresa_annex_merge['_merge'].isin(['left_only'])]

    # Create a subset of the maresa_annex_merge which only contains the annex with MarESA assessments
    annex_maresa = maresa_annex_merge.loc[maresa_annex_merge['_merge'].isin(['both'])]

    #############################################################

    # Step 1:
    # Assign a full set of pressures with the value 'Unknown' to all annex data which do not have MarESA Assessments

    # Create subset of all unique pressures and NE codes to be used for the append. Achieve this by filtering the MarESA
    # DataFrame to only include the unique pressures from the pressures list.
    PressuresCodes = MarESA.drop_duplicates(subset=['NE_Code', 'Pressure'], inplace=False)
    PressuresCodes = ml.fill_unknown_cols(PressuresCodes)

    # Perform cross join to blanket all pressures with unknown values to all EUNIS codes within the correlation_snippet
    annex_unknown_template_cjoin = ml.df_crossjoin(annex_only, PressuresCodes)

    # Rename columns to match MarESA data
    annex_unknown_template_cjoin.rename(columns={
        'Pressure_y': 'Pressure', 
        'Resistance_y': 'Resistance',
        'Resilience_y': 'Resilience', 
        'Sensitivity_y': 'Sensitivity', 
        'JNCC_Code_y': 'JNCC_Code'
        }, inplace=True)

    columns_of_interest = ['SubregionName', 'JNCC_Code', 'Annex I habitat', 
                           'Annex I sub-feature', 'Classification level', 'EUNIS code',
                           'EUNIS name', 'JNCC code', 'JNCC name', 'Pressure',
                           'Resilience', 'Resistance', 'Sensitivity']

    # keeping only the columns of interst of the two columns so
    # they can be appended
    annex_unknown = annex_unknown_template_cjoin[columns_of_interest]
    annex_maresa = annex_maresa[columns_of_interest]
    # Append the annex_unknown into the refined annex_maresa DF
    annex_maresa_unknowns = annex_maresa.append(annex_unknown, ignore_index=True)

    #############################################################

    # Step 2:
    # Prepare the data for aggregation analyses

    # Reformat contents of assessment columns within DF
    reformat_ev = {"Not relevant (NR)": "Not relevant",
                   "No evidence (NEv)": "No evidence",
                   "Not assessed (NA)": "Not assessed",
                   "Not Assessed (NA)": "Not assessed"}
    
    annex_maresa_unknowns.replace(reformat_ev, inplace=True)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    annex_maresa_unknowns['Sensitivity'].fillna(value='', inplace=True)

    # Subset the annex_maresa_unknowns DF to only retain the columns of interest
    annex_maresa_unknowns = annex_maresa_unknowns[[
        'SubregionName', 'JNCC_Code', 'Annex I habitat', 'Annex I sub-feature', 'Classification level', 'EUNIS code',
         'EUNIS name', 'JNCC code', 'JNCC name', 'Pressure', 'Resilience', 'Resistance', 'Sensitivity'
    ]]

    ####################################################################################################################

    # combine columns to give something to groupby on
    annex_maresa_unknowns['unique_id'] = annex_maresa_unknowns['SubregionName'].astype(str) + ':'\
                                         + annex_maresa_unknowns['Annex I habitat'].astype(str) + ':'\
                                         + annex_maresa_unknowns['Annex I sub-feature'].astype(str)

    # Group all L6 data by Sensitivity values
    maresa_annex_agg = annex_maresa_unknowns.groupby([
        'Pressure', 'unique_id'
    ])['Sensitivity'].apply(lambda x: ', '.join(x)).reset_index(inplace=False)

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    maresa_annex_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
                  'Unknown']] = maresa_annex_agg.apply(lambda df: pd.Series(ml.counter(df['Sensitivity'])), axis=1)

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
        maresa_annex_agg[value] = maresa_annex_agg[key]

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for key in colNames:
        maresa_annex_agg[key] = maresa_annex_agg[key].apply(lambda x: ml.replacer(x, key))

    ####################################################################################################################

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    maresa_annex_agg['AggregatedSensitivity'] = maresa_annex_agg.apply(lambda df: ml.final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    maresa_annex_agg['AssessedCount'] = maresa_annex_agg.apply(lambda df: ml.combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    maresa_annex_agg['UnassessedCount'] = maresa_annex_agg.apply(lambda df: ml.combine_unassessedcounts(df), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    maresa_annex_agg['AggregationConfidenceValue'] = maresa_annex_agg.apply(lambda df: ml.create_confidence(df), axis=1)

    # Create categories for confidence values: EUNIS Level 2
    maresa_annex_agg['AggregationConfidenceScore'] = maresa_annex_agg.apply(
        lambda df: ml.categorise_confidence(df, 'AggregationConfidenceValue'), axis=1)

    ####################################################################################################################

    # Split the unique_id column back into the individual columns once aggregated by unique combination
    # of Feature and Sub-Feature.
    maresa_annex_agg[['Bioregion', 'Annex I habitat', 'Annex I sub-type']] = maresa_annex_agg.apply(lambda row: ml.split_id3(row), axis='columns', result_type='expand')

    # Rearrange columns correctly into DataFrame schema
    maresa_annex_agg = maresa_annex_agg[[
        'Pressure', 'Bioregion', 'Annex I habitat', 'Annex I sub-type',
        'AggregatedSensitivity', 'AssessedCount', 'UnassessedCount',
        'AggregationConfidenceValue', 'AggregationConfidenceScore'
    ]]

    # Remove all data from the DF which is flagged to be NaN within the 'Annex I habitat' column
    maresa_annex_agg = maresa_annex_agg[maresa_annex_agg['Annex I habitat'] != 'nan']

    # Export data

    # Define folder file path to be saved into
    outpath = "./Output/"
    maresa_version = ml.get_file_v(marESA_file, 'marESA')
    filename = "AnxI_Scot_Off_Sens_Agg_" + (time.strftime("%Y%m%d") + '_' + str(maresa_version) + ".csv")
    # Run the output DF.to_csv method
    maresa_annex_agg.to_csv(outpath + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and
    #round value to 1 decimal place.
    print('...The ' + str(filename) + ' script took ' +
        str(round(elapsed / 60, 1)) + ' minutes to run and complete.' +
           '\n' + 'This has been saved as a time-stamped output at ' +
           'the following filepath: ' + str(outpath) + '\n\n')


if __name__ == "__main__":

    main('MarESA-Data-Extract-habitatspressures_2022-04-20.csv', 'Scottish_Offshore_AnnexI_2022-03-16.csv')
