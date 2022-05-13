import pandas as pd
import numpy as np


def fill_missing_maresa_rows(df):
    '''There is a lot of missing data in the maresa extract. either
    lines that are blank instead of unknown or missing lines. This
    creates lines filled with unknowns for these
    '''

    df.drop_duplicates(['habitatID', 'Pressure'], inplace=True)

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
    
    df_final = remove_key_rows(df_final)

    return(df_final)


def df_crossjoin(df1, df2):
    """
    Make a cross join (cartesian product) between two dataframes by using a constant temporary key.
    Also sets a MultiIndex which is the cartesian product of the indices of the input dataframes.
    :param df1 dataframe 1
    :param df1 dataframe 2

    :return cross join of df1 and df2
    """
    try:
        df1.loc[:, '_tmpkey'] = 1
        df2.loc[:, '_tmpkey'] = 1
    except:
        return(pd.DataFrame(columns=['SubregionName', 'JNCC_Code', 'Annex I habitat', 'Annex I sub-feature', 'Classification level', 'EUNIS code',
        'EUNIS name', 'JNCC code', 'JNCC name', 'Pressure', 'Resilience', 'Resistance', 'Sensitivity']))

    res = pd.merge(df1, df2, on='_tmpkey').drop('_tmpkey', axis=1)

    res.index = pd.MultiIndex.from_product((df1.index, df2.index))

    df1.drop('_tmpkey', axis=1, inplace=True)
    df2.drop('_tmpkey', axis=1, inplace=True)

    return res


######################Counter functions #######################################
# can these be homogenised?

# Define function to count the number of assessment values
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

# Define all functions which are required within the script to execute aggregation process
# Function Title: counter
def counter_rt(value):
    """Count the total no. of occurrences of each resistance (high, medium, low, not sensitive, not relevant,
        no evidence, not assessed, unknown
        Return values to be assigned to new columns through lambda function"""
    counthigh = value.count('High')
    countmedium = value.count('Medium')
    countlow = value.count('Low')
    countnone = value.count('None')
    countns = value.count('Not sensitive')
    countnr = value.count('Not relevant')
    countne = value.count('No evidence')
    countna = value.count('Not assessed')
    countuk = value.count('Unknown')
    return counthigh, countmedium, countlow, countnone, countns, countnr, countne, countna, countuk


# Define all functions which are required within the script to execute aggregation process
# Function Title: counter
def counter_rl(value):
    """Count the total no. of occurrences of each resilience (high, medium, low, not sensitive, not relevant,
        no evidence, not assessed, unknown
        Return values to be assigned to new columns through lambda function"""
    counthigh = value.count('High')
    countmedium = value.count('Medium')
    countlow = value.count('Low')
    countvlow = value.count('Very')
    countns = value.count('Not sensitive')
    countnr = value.count('Not relevant')
    countne = value.count('No evidence')
    countna = value.count('Not assessed')
    countuk = value.count('Unknown')
    return counthigh, countmedium, countlow, countvlow, countns, countnr, countne, countna, countuk

############################################################################

# Define replacer() function to fill a numerical with the repstring being analysed
# Function Title: replacer
def replacer(value, repstring):
    """Perform string replace on each sensitivity count column (one set of duplicates only)"""
    if value == 0:
        return 'NA'
    elif value != 0:
        return repstring


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
        # if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in nsens:
        # if 'Not relevant' in nrel:
        #     nr = 'Not Applicable'
        #     value.append(nr)
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


def categorise_confidence(df, column):
    """Partition and categorise confidence values by quantile intervals"""
    if df[column] < 0.33:
        return 'Low'
    elif df[column] >= 0.33 and df[column] < 0.66:
        return ' Medium'
    elif df[column] >= 0.66:
        return 'High'


def split_id3(row):
    return(row['unique_id'].split(':')[0],
            row['unique_id'].split(':')[1],
            row['unique_id'].split(':')[2])


def split_id2(row):
    return(row['unique_id'].split(':')[0],
            row['unique_id'].split(':')[1])


def get_file_v(file_name, pref):
    '''gets the maresa version from the filename'''
    # Re-split the string to remove the .csv file extension
    file_date = str(file_name).split('.')[0]
    # Re-split the file name to only retain the date of creation
    file_date = str(file_date).split('_')[-1]
    # remove the hyphens
    file_date = file_date.replace('-', '')
    # Create an abreviated version of the filename with the date
    return(pref + file_date)


def clean_df(df):
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    return(df)


def fill_unknown_cols(df):
    """The first habitat is taken for all the variations of pressures
    it has values filled so these need to be set to unknown
    """
    
    fill_u_cols = ['Resistance', 'ResistanceQoE', 'ResistanceAoE', 'ResistanceDoE', 
                'Resilience', 'ResilienceQoE', 'ResilienceAoE',  'resilienceDoE',  
                'Sensitivity',  'SensitivityQoE',  'SensitivityAoE', 'SensitivityDoE']
    df.loc[:, fill_u_cols] = 'Unknown'

    fill_n_cols = ['EUNIS_Code', 'Name', 'JNCC_Name', 'JNCC_Code', 'EUNIS level']
    df.loc[:, fill_n_cols] = np.nan

    return(df)

################ sensitivity ###################

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
        if 'Unknown' in un:
            unk = 'Unknown'
            value.append(unk)

    s = ', '.join(value)
    return str(s)


################Features######################

# Create function which takes the Annex I Feature/SubFeature columns, and combines both entries into a single column
# This enables the data to be grouped and aggregated using the .groupby() function (does not support multiple
# simultaneous aggregations)
def together(row):
    # Pull in data from both columns of interest
    subb_r = row['SubregionName']
    ann1 = row['Annex I habitat']
    sub_f = row['Annex I sub-feature']
    # Return a string of both individual targets combined by a ' - ' symbol
    return str(subb_r) + ' - ' + str(str(ann1) + ' - ' + str(sub_f))


############ Aggregations #####################

def eunis_lvl(row):
    """User defined function to pull out all data from the column 'EUNIS_Code' and return an integer dependant on
    the EUNIS level in response"""

    # Create object oriented variable to store EUNIS_Code data
    ecode = str(row['EUNIS_Code'])
    # Create if / elif conditions to produce response dependent on the string length of the inputted data
    if len(ecode) == 1:
        return '1'
    elif len(ecode) == 2:
        return '2'
    elif len(ecode) == 4:
        return '3'
    elif len(ecode) == 5:
        return '4'
    elif len(ecode) == 6:
        return '5'
    elif len(ecode) == 7:
        return '6'









































# # Function Title: combine_assessedcounts
# def combine_assessedcounts(df):
#     """Conditional statements which combine assessed count data and return as string value"""
#     # Create object oriented variable for each column of data from DataFrame (assessed only)
#     high = df['High']
#     med = df['Medium']
#     low = df['Low']
#     none = df['None']
#     nsens = df['Not sensitive']
#     # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
#     nrel = df['Not relevant']
#     nev = df['No evidence']
#     n_ass = df['Not assessed']
#     un = df['Unknown']

#     # Create empty list for all string values to be appended into - this will be assigned to each field when data
#     # are iterated through using the lambdas function which follows immediately after this function
#     value = []
#     # Create series of conditional statements to append string values into the empty list ('value') if conditional
#     # statements are fulfilled
#     if 'High' in high:
#         h = 'H(' + str(df['Count_High']) + ')'
#         value.append(h)
#     if 'Medium' in med:
#         m = 'M(' + str(df['Count_Medium']) + ')'
#         value.append(m)
#     if 'Low' in low:
#         lo = 'L(' + str(df['Count_Low']) + ')'
#         value.append(lo)
#     if 'None' in none:
#         n = 'N(' + str(df['Count_None']) + ')'
#         value.append(n)
#     if 'Not sensitive' in nsens:
#         ns = 'NS(' + str(df['Count_NotSensitive']) + ')'
#         value.append(ns)
#     if 'Not relevant' in nrel:
#         nr = 'NR(' + str(df['Count_NotRel']) + ')'
#         value.append(nr)
#     if 'NA' in high and 'NA' in med and 'NA' in low and 'NA' in none and 'NA' in nsens and 'NA' in nrel:
#         if 'No evidence' in nev:
#             ne = 'Not Applicable'
#             value.append(ne)
#         if 'Not assessed' in n_ass:
#             nass = 'Not Applicable'
#             value.append(nass)
#         if 'Unknown' in un:
#             unk = 'Not Applicable'
#             value.append(unk)
#     s = ', '.join(set(value))
#     return str(s)




# # Function Title: combine_unassessedcounts
# def combine_unassessedcounts(df):
#     """Conditional statements which combine unassessed count data and return as string value"""
#     # Create object oriented variable for each column of data from DataFrame (assessed only)
#     # Create object oriented variable for each column of data from DataFrame (not assessment criteria only)
#     nrel = df['Not relevant']
#     nev = df['No evidence']
#     n_ass = df['Not assessed']
#     un = df['Unknown']

#     # Create empty list for all string values to be appended into - this will be assigned to each field when data
#     # are iterated through using the lambdas function which follows immediately after this function

#     values = []

#     # Create series of conditional statements to append string values into the empty list ('value') if conditional
#     # statements are fulfilled

#     # if 'Not relevant' in nrel:
#     #     nr = 'NR(' + str(df['Count_NotRel']) + ')'
#     #     values.append(nr)
#     if 'No evidence' in nev:
#         ne = 'NE(' + str(df['Count_NoEvidence']) + ')'
#         values.append(ne)
#     if 'Not assessed' in n_ass:
#         na = 'NA(' + str(df['Count_NotAssessed']) + ')'
#         values.append(na)
#     if 'Unknown' in un:
#         unk = 'UN(' + str(df['Count_Unknown']) + ')'
#         values.append(unk)
#     # if 'NA' in nrel and 'NA' in nev and 'NA' in n_ass and 'NA' in un:
#     if 'NA' in nev and 'NA' in n_ass and 'NA' in un:
#         napp = 'Not Applicable'
#         values.append(napp)
#     s = ', '.join(set(values))
#     return str(s)



# # Function Title: categorise_confidence
# def categorise_confidence(df, column):
#     """Partition and categorise confidence values by quantile intervals"""
#     if column == 'L2_AggregationConfidenceValue':
#         value = df[column]
#         if value < 0.33:
#             return 'Low'
#         elif value >= 0.33 and value < 0.66:
#             return ' Medium'
#         elif value >= 0.66:
#             return 'High'
#     elif column == 'L3_AggregationConfidenceValue':
#         value = df[column]
#         if value < 0.33:
#             return 'Low'
#         elif value >= 0.33 and value < 0.66:
#             return ' Medium'
#         elif value >= 0.66:
#             return 'High'
#     elif column == 'L4_AggregationConfidenceValue':
#         value = df[column]
#         if value < 0.33:
#             return 'Low'
#         elif value >= 0.33 and value < 0.66:
#             return ' Medium'
#         elif value >= 0.66:
#             return 'High'
#     elif column == 'L5_AggregationConfidenceValue':
#         value = df[column]
#         if value < 0.33:
#             return 'Low'
#         elif value >= 0.33 and value < 0.66:
#             return ' Medium'
#         elif value >= 0.66:
#             return 'High'
#     elif column == 'L6_AggregationConfidenceValue':
#         value = df[column]
#         if value < 0.33:
#             return 'Low'
#         elif value >= 0.33 and value < 0.66:
#             return ' Medium'
#         elif value >= 0.66:
#             return 'High'

# def str_split(row, str_interval):
#     # Import the target column into the local scope of the function
#     target_col = row['SubregionFeatureSubFeature']
#     # Split the target string to get the Feature using ' - ', as created with the 'together()' function earlier
#     # in the script
#     if str_interval == 'Subregion':
#         # Split the string and place both halves into a list
#         result = target_col.split(' - ')
#         # Slice the list to return the first of the two list items
#         return str(result[0])
#     if str_interval == 'Feature':
#         # Split the string and place both halves into a list
#         result = target_col.split(' - ')
#         # Slice the list to return the first of the two list items
#         return str(result[1])
#     # Split the target string to get the SubFeature using ' - ', as created with the 'together()' function earlier
#     # in the script
#     if str_interval == 'SubFeature':
#         # Split the string and place both halves into a list
#         result = target_col.split(' - ')
#         # Slice the list to return the second of the two list items
#         return str(result[2])


# # Run the str_split() function to return the combined Feature data back into two separate columns
# maresa_annex_agg['Bioregion'] = maresa_annex_agg.apply(lambda row: str_split(row, 'Subregion'), axis=1)

# # Run the str_split() function to return the combined Feature data back into two separate columns
# maresa_annex_agg['Annex I Habitat'] = maresa_annex_agg.apply(lambda row: str_split(row, 'Feature'), axis=1)

# # Run the str_split() function to return the combined SubFeature data back into two separate columns
# maresa_annex_agg['Annex I sub-type'] = maresa_annex_agg.apply(lambda row: str_split(row, 'SubFeature'), axis=1)