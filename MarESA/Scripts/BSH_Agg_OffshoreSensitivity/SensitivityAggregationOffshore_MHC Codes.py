########################################################################################################################

# Title: JNCC MarESA Sensitivity Aggregation (EUNIS)

# Authors: Matear, L.(2019)                                                           Email: marinepressures@jncc.gov.uk
# Version Control: 1.0

# Script description:    Aggregate MarESA sensitivity assessments for UK offshore biotopes on spatial location and
#                        habitat classification system. For full detail of the methods used, please see:
#                        https://hub.jncc.gov.uk/assets/faa8722e-865d-4d9f-ab0b-15a2eaa77db0

#                        For any enquiries please contact marinepressures@jncc.gov.uk

########################################################################################################################

#                                      A. MarESA Preparation: Unknowns Automation                                      #

########################################################################################################################

# Import libraries used within the script, assign a working directory and import data

# Import all Python libraries required
import os
import time
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

#############################################################

# remove the unknowns for the climate change pressures for the following level 6 biotopes: A5.5111, A5.5112 and A5.3611



# Define the code as a function to be executed as necessary
def main(marESA_file, bioregions_ext,output_file):
    # Test the run time of the function
    start = time.process_time()
    print('Offshore sensitivity aggregation script started...')

    # Loading all data into the IDE
    # Setting a working directory for input data set
    #os.chdir(r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\
    #Aggregation_InputData\Unknowns_InputData")

    # Import the JNCC Correlation Table as Pandas DataFrames - updated
    # with CorrelationTable_C16042020
    CorrelationTable = pd.read_excel("./MarESA/Data/CorrelationTable_C16042020.xlsx",
        'Correlations', dtype=str)

    # Import all data within the MarESA extract as Pandas DataFrame
    # NOTE: This must be updated each time a new MarESA Extract is released
    # The top copy of the MarESA Extract can be found at the following
    # file path:
    # \\jncc-corpfile\gis\Reference\Marine\Sensitivity
    #MarESA = pd.read_excel("./MarESA/Data/" + marESA_file,
    #                       marESA_tab,
    #                       dtype={'EUNIS_Code': str},
    #                       engine='xlrd')
    MarESA = pd.read_csv("./MarESA/Data/" + marESA_file,
                           dtype={'JNCC_Code': str,'EUNIS_Code': str})

    # OG 07/02/2023 Remove temporary EUNIS Codes as these are duplicates of existing EUNIS 2008 codes.
    MarESA = MarESA[~MarESA['EUNIS_Code'].str.contains('TMP', na = False)]

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

    # Subset data set to only comprise values where the listed biotopes
    # value is not recorded as False or 'nan'
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['UK Habitat'].isin(['False'])]
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['UK Habitat'].isin(['nan'])]

    # Drop any unknown values from the CorrelationTable DF
    CorrelationTable.dropna(subset=['UK Habitat'], inplace=True)

    # Subset data set to exclude any EUNIS level 1, 2 and 3 data as these do not have associated sensitivity
    # assessments
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['EUNIS level'].isin(['1', '2', '3'])]

    # break down the rows with / in
    CorrelationsBreakdown = CorrelationTable[CorrelationTable['JNCC 15.03 code'].notnull()]
    CorrelationsBreakdown = CorrelationsBreakdown[CorrelationsBreakdown['JNCC 15.03 code'].str.contains('/')]

    # remove those from CorrelationsBreakdown
    CorrelationTable = CorrelationTable.loc[~CorrelationTable['JNCC 15.03 code'].str.contains('/', na=False)]

    CorrelationsBreakdown.reset_index(inplace=True)

    # deconstruct each row and add to a new table
    CorrelationsReconstructed=pd.DataFrame(columns=CorrelationsBreakdown.columns)
    for i in range(0,len(CorrelationsBreakdown)):
        EUNIScode = CorrelationsBreakdown['EUNIS code 2007'][i]
        EUNISlevel = CorrelationsBreakdown['EUNIS level'][i]
        EUNISname = CorrelationsBreakdown['EUNIS name 2007'][i]
        UKHABITAT = CorrelationsBreakdown['UK Habitat'][i]
        JNCCCODE = CorrelationsBreakdown['JNCC 15.03 code'][i]
        JNCCNAME = CorrelationsBreakdown['JNCC 15.03 name'][i]
        JNCCLEVEL = CorrelationsBreakdown['JNCC 15.03 level'][i]
        SUBTYPE = CorrelationsBreakdown['SNH Annex I sub-type'][i]

        NoSlashes=JNCCCODE.count('/')
        for n in range(0,NoSlashes+1):
            JNCC_code_iteration=JNCCCODE.split(' / ')[n]
            JNCC_name_iteration=JNCCNAME.split(' / ')[n]
            df_temp=pd.DataFrame({'EUNIS code 2007':[EUNIScode],'EUNIS level':[EUNISlevel],'EUNIS name 2007':[EUNISname],'UK Habitat':[UKHABITAT],'JNCC 15.03 code':[JNCC_code_iteration],
                               'JNCC 15.03 name':[JNCC_name_iteration],'JNCC 15.03 level':[JNCCLEVEL],'SNH Annex I sub-type':[SUBTYPE]})
            CorrelationsReconstructed=CorrelationsReconstructed.append(df_temp)


    CorrelationTable = CorrelationTable.append(CorrelationsReconstructed)
    CorrelationTable.reset_index(inplace=True)


    #############################################################

    # Formatting the MarESA DF

    # Adding a EUNIS level column to the DF based on the 'EUNIS_Code' column
    # Function Title: eunis_lvl
    def MHC_lvl(row):
        """User defined function to pull out all data from the column 'EUNIS_Code' and return an integer dependant on
        the EUNIS level in response"""

        # Create object oriented variable to store EUNIS_Code data
        ecode = str(row['JNCC_Code'])
        # Create if / elif conditions to produce response dependent on the string length of the inputted data
        if ecode.count('.') == 0:
            return '2'
        elif ecode.count('.') == 1:
            return '3'
        elif ecode.count('.') == 2:
            return '4'
        elif ecode.count('.') == 3:
            return '5'
        elif ecode.count('.') == 4:
            return '6'



    # Adding a EUNIS level column to the DF based on the 'EUNIS_Code' column - using the function
    MarESA['MHC level'] = MarESA.apply(lambda row: MHC_lvl(row), axis=1)

    MarESA['EUNIS level'] = MarESA['MHC level']

    # Subset data set to exclude any EUNIS level 1, 2 and 3 data as these do not have associated sensitivity
    # assessments
    MarESA = MarESA.loc[~MarESA['EUNIS level'].isin(['1', '2', '3'])]

    #############################################################

    # Identifying the unique EUNIS codes within each data set

    # Create list of all unique values within the CorrelationTable DF
    CorrelationTable_MHC = list(CorrelationTable['JNCC 15.03 code'].unique())
    
    # Create list of all unique values within the MarESA DF
    MarESA_MHC = list(MarESA['JNCC_Code'].unique())

    # 3.3 Create a list of all unique EUNIS codes which are present within the CorrelationTableDF, but not the MarESA DF
    MHC_Difference = list(set(CorrelationTable_MHC) - set(MarESA_MHC))

    # Adding the CorrelationTable DF EUNIS data not in the MarESA DF as 'Unknown' values

    # remove nan
    MHC_Difference = [x for x in MHC_Difference if str(x) != 'nan']

    # Sub-setting the CorrelationTable to only include the EUNIS codes identified within the EUNIS_Difference list
    CorrelationTable_Subset = CorrelationTable.loc[CorrelationTable['JNCC 15.03 code'].isin(MHC_Difference)]

    # Renaming the columns within the CorrelationTable_Subset DF to match the relevant MarESA columns
    CorrelationTable_Subset.rename(columns={'EUNIS code 2007': 'EUNIS_Code', 'EUNIS name 2007': 'JNCC_Name',
                                            'JNCC 15.03 code': 'JNCC_Code'}, inplace=True)

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
    CorrelationEUNIS_list = list(CorrelationTable['JNCC 15.03 code'].unique())

    # remove nan
    CorrelationEUNIS_list = [x for x in CorrelationEUNIS_list if str(x) != 'nan']

    # Create snippet of the correlation table including only the unique biotope codes which do not exist in the MarESA
    # data
    correlation_snippet = CorrelationTable_Subset.loc[CorrelationTable_Subset['JNCC_Code'].isin(CorrelationEUNIS_list)]

    # Remove unwanted erroneous biotope codes from the correlation_snippet data
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'LS.LMp.Sm.SM16._']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'LS.LMp.Sm.SM13._']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'LS.LMp.Sm_']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'SS.SCS.SCSVS']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code != 'Saltmarsh 5 EUNIS types Sm:']
    correlation_snippet = correlation_snippet[correlation_snippet.EUNIS_Code !=
                                              '104 EUNIS level 5 and 6 types 26 NVC types:']

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
    correlation_snippet_template = df_crossjoin(correlation_snippet, PressuresCodes)

    # Drop unwanted columns from the correlation_snippet_template data
    correlation_snippet_template.drop(['JNCC_Code_y', 'JNCC_Name_y', 'EUNIS_Code_y', 'EUNIS level_y'], axis=1,
                                      inplace=True)

    # Rename columns to match MarESA data
    correlation_snippet_template.rename(columns={'EUNIS_Code_x': 'EUNIS_Code', 'EUNIS level_x': 'EUNIS level',
                                                 'JNCC_Code_x': 'JNCC_Code', 'JNCC_Name_x': 'JNCC_Name'}, inplace=True)

    # Order columns to match MarESA data
    correlation_snippet_template = correlation_snippet_template[[
        'JNCC_Code', 'JNCC_Name', 'EUNIS_Code', 'Name',
        'NE_Code', 'Pressure', 'Resistance', 'ResistanceQoE',
        'ResistanceAoE', 'ResistanceDoE', 'Resilience',
        'ResilienceQoE', 'ResilienceAoE', 'resilienceDoE',
        'Sensitivity', 'SensitivityQoE', 'SensitivityAoE',
        'SensitivityDoE', 'url', 'EUNIS level'
        ]]

    # Append the correlation_snippet_template into the MarESA data to have a MarESA dataset which accounts for
    # 'unknown' values.
    maresa = MarESA.append(correlation_snippet_template, ignore_index=True)

    # convert eunis level back to MHC level
    maresa['MHC level'] = maresa['EUNIS level']


    ####################################################################
    #
    #                   B. MarESA Aggregation Process
    #
    ####################################################################

    # Introduction

    # This script allows the user to aggregate sensitivity data across varying tiers of EUNIS hierarchies.
    # The main purpose of this code is to develop a mechanism through which the user can identify which completed
    # assessments sit within which tiers of the EUNIS hierarchy. The outputs of these analyses should enable the user to
    # map the spatial distribution of assessments made at EUNIS Levels 5 and 6 at less detailed resolutions
    # (e.g. EUNIS Level 2 and / or EUNIS Level 3).

    # For an overview of the aggregation process, please see the 'Methodology Infographic' file at the following
    # web URL:
    # https://github.com/jncc/pressures-and-impacts/tree/master/Sensitivity%20Assessments/Methodology%20Infographic

    ####################################################################

    # Initial setup and data import

    # Load required data and assign to object oriented variables

    # Run short analysis to identify the most recently created iteration of the BioregionsExtract and read this in for
    # the aggregation process

    # Define a directory to be searched
    #bioreg_dir = r"J:\GISprojects\Marine\Sensitivity\MarESA aggregation\Aggregation_InputData\Aggregation_InputData\BioregionsExtract"
    # Set this as the working directory
    #os.chdir(bioreg_dir)
    # Read in all files within this target directory
    #bioreg_files = filter(os.path.isfile, os.listdir(bioreg_dir))
    # Add full filepaths to the identified files within said directory
    #bioreg_files = [os.path.join(bioreg_dir, f) for f in bioreg_files]
    # Sort all files read in by the most recently edited first
    #bioreg_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Define the bioregions object containing the updated outputs from
    # the Bioregions 2017 Contract - read the last
    # edited file

    bioregions = pd.read_excel('./MarESA/Data/' + bioregions_ext, dtype=str)

    ############################################################

    # Create an object variable storing the date of the bioregions
    # version used - this is entered into the aggregation
    # output file name for QC purposes.

    # Re-split the string to remove the .csv file extension
    bioreg_date = str(bioregions_ext).split('.')[0]
    # Re-split the file name to only retain the date of creation
    bioreg_date = str(bioreg_date).split('_')[-1]
    # Create an abreviated version of the filename with the date
    bioreg_version = 'Bioreg' + bioreg_date

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

    ############################################################

    # Develop a subset of the bioregions data which only contains EUNIS codes of string length 4 or greater
    # This will remove any unwanted EUNIS L1 - L3 from the data
    # Convert bioregions 'HabitatCode' to string
    bioregions['HabitatCode'] = bioregions['HabitatCode'].astype(str)
    bioregions = bioregions[bioregions['HabitatCode'].map(len) >= 5]

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
        df['JNCC_Code'].str.strip()
        df['Sensitivity'].replace(["Not relevant (NR)"], "Not relevant", inplace=True)
        df['Sensitivity'].replace(["No evidence (NEv)"], "No evidence", inplace=True)
        df['Sensitivity'].replace(["Not assessed (NA)"], "Not assessed", inplace=True)
        df['Resistance'].replace(["Not Assessed (NA)"], "Not assessed", inplace=True)

        return df

    # Function Title: unwanted_char
    def unwanted_char(df, column):
        """User defined function to refine dataset - remove unwanted special characters and acronyms from sensitivity
        scores. User must pass the DataFrame and the column (as a string) as the arguments to the parentheses of the
        function"""
        for eachField in df[column]:
            eachField.replace(r"\\s*zz(][^\\]+\\)", "")
        return df

    # Function Title: eunis_col
    def JNCC_col(row):
        """User defined function to pull out all entries in EUNIS_Code column and create returns based on string
        slices of the EUNIS data. This must be used with df.apply() and a lambda function.

        e.g. bioreg_maresa_merge[['Level_1', 'Level_2', 'Level_3',
              'Level_4', 'Level_5', 'Level_6']] = bioreg_maresa_merge.apply(lambda row: pd.Series(eunis_col(row)), axis=1)"""

        # Create object oriented variable to store EUNIS_Code data
        ecode = str(row['JNCC_Code'])
        # Create if / elif conditions to produce response dependent on the string length of the inputted data.
        if ecode.count('.') == 0:
            return ecode, None, None, None, None
        elif ecode.count('.') == 1:
            return ecode.split('.')[0], ecode.split('.')[0]+'.'+ecode.split('.')[1], None, None, None 
        elif ecode.count('.') == 2:
            return ecode.split('.')[0], ecode.split('.')[0]+'.'+ecode.split('.')[1], ecode.split('.')[0]+'.'+ecode.split('.')[1]+'.'+ecode.split('.')[2], None, None
        elif ecode.count('.') == 3:
            return ecode.split('.')[0], ecode.split('.')[0]+'.'+ecode.split('.')[1], ecode.split('.')[0]+'.'+ecode.split('.')[1]+'.'+ecode.split('.')[2], ecode.split('.')[0]+'.'+ecode.split('.')[1]+'.'+ecode.split('.')[2]+'.'+ecode.split('.')[3], None
        elif ecode.count('.') == 4:
            return ecode.split('.')[0], ecode.split('.')[0]+'.'+ecode.split('.')[1], ecode.split('.')[0]+'.'+ecode.split('.')[1]+'.'+ecode.split('.')[2], ecode.split('.')[0]+'.'+ecode.split('.')[1]+'.'+ecode.split('.')[2]+'.'+ecode.split('.')[3], ecode.split('.')[0]+'.'+ecode.split('.')[1]+'.'+ecode.split('.')[2]+'.'+ecode.split('.')[3]+'.'+ecode.split('.')[4]


    ####################################################################################################################

    # Defining functions (aggregation process)

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

    # Function Title: replacer
    def replacer(value, repstring):
        """Perform string replace on each sensitivity count column (one set of duplicates only)"""
        if value == 0:
            return 'NA'
        elif value != 0:
            return repstring

    # Function Title: create_sensitivity
    def create_sensitivity(df):
        """Series of conditional statements which return a string value of all assessment values
        contained within individual columns"""
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
        value2=[]
        value=set(value)
        order=['H','M','L','NS','NR']
        if 'Not Applicable' in value:
            value2=value.copy()
        else:
            for i in order:
                for x in value:
                    a=x[:x.index("(")]
                    if a ==i:
                        value2.append(x)
        s = ', '.join(value2)
        return str(s)

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

    # Function Title: categorise_confidence
    def categorise_confidence(df, column):
        """Partition and categorise confidence values by quantile intervals"""
        if column == 'L2_AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'
        elif column == 'L3_AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'
        elif column == 'L4_AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'
        elif column == 'L5_AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'
        elif column == 'L6_AggregationConfidenceValue':
            value = df[column]
            if value < 0.33:
                return 'Low'
            elif value >= 0.33 and value < 0.66:
                return ' Medium'
            elif value >= 0.66:
                return 'High'

    # Function Title: column5
    def column_aggregate(df, column):
        """Sample Level_5 column and return string variables sliced within the range [0:6]"""
        value = df[column]
        length = value.count('.')
        if length == 4:
            sample = value.split('.')[0]+'.'+value.split('.')[1]+'.'+value.split('.')[2]+'.'+value.split('.')[3]
        elif length == 3:
            sample = value.split('.')[0]+'.'+value.split('.')[1]+'.'+value.split('.')[2]
        elif length == 2:
            sample = value.split('.')[0]+'.'+value.split('.')[1]
        elif length == 1:
            sample = value.split('.')[0]
        return sample
    
    ####################################################################################################################

    # Data formatting

    # Remove unnecessary whitespace, rename Bioregions column and merge data sets

    # Rename bioregions column to facilitate merge
    bioregions.rename(columns={'HabitatCode': 'EUNIS_Code'}, inplace=True)

    # Merge bioregions and marESA data together
    bioreg_maresa_merge = pd.merge(bioregions, maresa, on='EUNIS_Code', how='left')

    # Refine the DF to remove the currently not needed Region 8 (deep dea)
    bioreg_maresa_merge = bioreg_maresa_merge[bioreg_maresa_merge['SubregionName'] != 'Region 8 (deep-sea)']

    # Remove unwanted data by passing the bioreg_maresa_merge DF to the df_clean() function
    df_clean(bioreg_maresa_merge)

    # Fill NaN values with empty string values to allow for string formatting with unwanted_char() function
    bioreg_maresa_merge['Sensitivity'].fillna(value='', inplace=True)

    # Remove unwanted special characters from data by passing the bioreg_maresa_merge to the unwanted_char() function
    bioreg_maresa_merge = unwanted_char(bioreg_maresa_merge, 'Sensitivity')

    # Create individual EUNIS level columns in bioreg_maresa_merge using a lambda function and apply() method on
    # 'EUNIS_Code' column on DF.
    bioreg_maresa_merge[['Level_2', 'Level_3', 'Level_4',
              'Level_5', 'Level_6']] = bioreg_maresa_merge.apply(lambda row: pd.Series(JNCC_col(row)), axis=1)

    # Create new 'EUNIS_Level' column which indicates the numerical value of the EUNIS level by passing the
    # bioreg_maresa_merge DF to the eunis_lvl() function.
    bioreg_maresa_merge['MHC_Level'] = bioreg_maresa_merge.apply(lambda row: MHC_lvl(row), axis=1)

    # Make codes with M. in start level 2 at M.X
    # Subset M codes
    M_codes=bioreg_maresa_merge[bioreg_maresa_merge['Level_2'] == 'M']
    # remove M codes from other dataset
    bioreg_maresa_merge= bioreg_maresa_merge[bioreg_maresa_merge['Level_2'] != 'M']
    # Restructure M codes
    M_codes['Level_2']=M_codes['Level_3']
    M_codes['Level_3']=M_codes['Level_4']
    M_codes['Level_4']=M_codes['Level_5']
    M_codes['Level_5']=M_codes['Level_6']
    M_codes['Level_6']='None'
    M_codes['MHC_Level']=M_codes['MHC_Level'].astype(int)-1
    M_codes['MHC_Level']=M_codes['MHC_Level'].astype(str)


    # merge back together
    bioreg_maresa_merge = bioreg_maresa_merge.append(M_codes)
    bioreg_maresa_merge

####################################################################################################################

    # Level 6 data only

    # Extract all level 6 data only and assign to object oriented variable to be aggregated to level 5
    original_L6_data = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['MHC_Level'].isin(['6'])])

    # Group all L6 data by Sensitivity values
    L6_processed = original_L6_data.groupby(['Level_6', 'Pressure', 'SubregionName'
                                             ])['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L6_processed = pd.DataFrame(L6_processed)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L6_processed = L6_processed.reset_index(inplace=False)

    # Reset columns within L6_processed DataFrame
    L6_processed.columns = ['Level_6', 'Pressure', 'SubregionName', 'Sensitivity']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L6_processed[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
                  'Unknown']] = L6_processed.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L6_processed['Count_High'] = L6_processed['High']
    L6_processed['Count_Medium'] = L6_processed['Medium']
    L6_processed['Count_Low'] = L6_processed['Low']
    L6_processed['Count_NotSensitive'] = L6_processed['Not sensitive']
    L6_processed['Count_NotRel'] = L6_processed['Not relevant']
    L6_processed['Count_NoEvidence'] = L6_processed['No evidence']
    L6_processed['Count_NotAssessed'] = L6_processed['Not assessed']
    L6_processed['Count_Unknown'] = L6_processed['Unknown']

    # Create colNames list for use with replacer() function
    colNames = ['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed', 'Unknown']

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score

    for eachCol in colNames:
        L6_processed[eachCol] = L6_processed[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Level 6 (aggregation)

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    L6_processed['L6_Sensitivity'] = L6_processed.apply(lambda df: create_sensitivity(df), axis=1)


    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    L6_processed['L6_FinalSensitivity'] = L6_processed.apply(lambda df: final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L6_processed['L6_AssessedCount'] = L6_processed.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L6_processed['L6_UnassessedCount'] = L6_processed.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Apply column5() function to L6_processed DataFrame to create new Level_5 column
    L6_processed['Level_5'] = L6_processed.apply(lambda df: pd.Series(column_aggregate(df, 'Level_6')), axis=1)



    # Use lambda function to apply create_confidence() function to the DataFrame
    L6_processed['L6_AggregationConfidenceValue'] = L6_processed.apply(lambda df: create_confidence(df), axis=1)

    # Drop unwanted data from L6_processed DataFrame
    L6_processed = L6_processed.drop([
        'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    # Remove child biotopes A5.7111 + A5.7112 from aggregation data due to prioritisation of Level 4 assessments.
    #L5_processed = L5_processed[L5_processed['Level_5'] != 'A5.7111']
    #L5_processed = L5_processed[L5_processed['Level_5'] != 'A5.7112']

    ####################################################################################################################

    ############# I AM HERE ##############

    # Creating an aggregated export

    # Create DataFrame for master DataFrame at end of script
    L6_export = L6_processed

    L6_export = L6_export.drop(['Sensitivity', 'L6_Sensitivity'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 6 to 5 aggregation (formatting) -

    # The following body of code begins the initial steps of the aggregation process from level 6 to level 5

    # Group data by Level_5, Pressure, SubregionName and apply resistance values to list using lambdas function and
    # .apply() method
    aggregated_L6_to_L5 = L6_processed.groupby(['Level_5', 'Pressure', 'SubregionName']
                                               )['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    aggregated_L6_to_L5 = pd.DataFrame(aggregated_L6_to_L5)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    aggregated_L6_to_L5 = aggregated_L6_to_L5.reset_index(inplace=False)

    # Reset columns within aggregated_L6_to_L5 DF
    aggregated_L6_to_L5.columns = ['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']

    # Extract all original level 5 data and assign to object oriented variable
    original_L5_data = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['MHC_Level'].isin(['5'])])

    # Assign data differences to new object oriented variable using outer merge between data frames
    assessed_L5L6_merge = pd.merge(original_L6_data, original_L5_data, how='outer', on=['Level_5', 'Pressure','SubregionName'],
                                   indicator=True)

    # Create object for L5 where there is no L6 - required for new edits added 27/02/2020
    assessed_L5_without_L6 = pd.DataFrame(assessed_L5L6_merge[assessed_L5L6_merge['Resistance_x'].isna()])
    assessed_L5_without_L6 = assessed_L5_without_L6[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity_y']]
    assessed_L5_without_L6.columns=['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']


    # Subset L5 original data to only retain data where there is no L6 child biotope
    # Create a list of the original L5 data which do not have associated child biotopes
    original_L5_without_L6_DF = pd.merge(original_L5_data, assessed_L5_without_L6, how='outer', on=['Level_5', 'Pressure','SubregionName'],indicator=True)
    original_L5_without_L6_DF = pd.DataFrame(original_L5_without_L6_DF[original_L5_without_L6_DF['_merge'] == 'both'])
    original_L5_without_L6_DF = original_L5_without_L6_DF[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity_y']]
    original_L5_without_L6_DF.columns=['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']


    # OG Changes 09/22 to allow for climate change pressures (if L6 unknown but L5 known then use L5 at L5)
    # create a new dataframe from which we will work out which L6 have all unknown values
    L6_Unknowns_Processing = L6_processed

    L6_Unknowns_Processing = L6_Unknowns_Processing.groupby(['Level_5', 'Pressure', 'SubregionName']
                                                            )['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L6_Unknowns_Processing = pd.DataFrame(L6_Unknowns_Processing)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L6_Unknowns_Processing = L6_Unknowns_Processing.reset_index(inplace=False)

    #Create a subset of all rows that contain only unknowns
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Not sensitive") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Medium") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("No evidence") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Not relevant") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Not assessed") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Low") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("High") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("None") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Very low") == False]
    L6_Unknowns_Processing=L6_Unknowns_Processing[L6_Unknowns_Processing['Sensitivity'].str.contains("Very high") == False]
    # Drop unwanted columns from L6 unknown DataFrame
    L6_Unknowns_Processing = L6_Unknowns_Processing[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']]
    # create final L6 unknons dataset
    L6_unknowns = L6_Unknowns_Processing


    # Create a new datatfram where we will work out which L5 have unknown values
    L5_knowns_processing = original_L5_data
    # Drop unwanted columns from 'original_L5_without_L6_DF' DataFrame
    L5_knowns_processing = L5_knowns_processing[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']]
    #Create a subset of all rows that contain only unknowns
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Not sensitive") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Medium") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("No evidence") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Not relevant") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Not assessed") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Low") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("High") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("None") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Very low") == False]
    L5_knowns_processing=L5_knowns_processing[L5_knowns_processing['Sensitivity'].str.contains("Very high") == False]
    # create final L5 unknons dataset
    L5_knowns_unknowns = L5_knowns_processing
    # Pull out all the L5 which arent in the unknown dataset - all known L5
    # Assign the unknown L5s and all the L5 data to new object oriented variable using outer merge between data frames
    original_L5_data_cleaned = original_L5_data[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']]
    L5_known_and_unknown = pd.merge(original_L5_data_cleaned, L5_knowns_unknowns, how='outer', on=['Level_5', 'Pressure','SubregionName','Sensitivity'],indicator=True)
    # Create object for L5 where there are no unknowns
    L5_known = pd.DataFrame(L5_known_and_unknown[L5_known_and_unknown['_merge'] == 'left_only'])
    # Drop unwanted columns from L5 known DataFrame
    L5_known = L5_known[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']]

    # Work out which L6 unknowns are L5 knowns
    # Assign the unknown L5s and all the L5 data to new object oriented variable using outer merge between data frames
    L6_Unknown_L5_known_processing = pd.merge(L6_unknowns, L5_known, how='outer', on=['Level_5', 'Pressure','SubregionName'],indicator=True)
    # Pull out the data which are known for L5 and unknown for L6
    L6_Unknown_L5_known = pd.DataFrame(L6_Unknown_L5_known_processing[L6_Unknown_L5_known_processing['_merge'] == 'both'])
    # Drop unwanted columns
    L6_Unknown_L5_known = L6_Unknown_L5_known[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity_y']]
    # rename the columns
    L6_Unknown_L5_known.columns = ['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']

    # remove the columns from L6 aggregated to L5 dataset where they are unknown and L5 is known
    # Merge L6_Unknown_L5_known with the aggregated_L6_to_L5 table
    aggregated_L6_to_L5_processing = pd.merge(aggregated_L6_to_L5, L6_Unknown_L5_known, how='outer', on=['Level_5', 'Pressure','SubregionName'],indicator=True)
    # drop unknown rows fro L6 to L5 aggregated data
    aggregated_L6_to_L5 = pd.DataFrame(aggregated_L6_to_L5_processing[aggregated_L6_to_L5_processing['_merge'] == 'left_only'])
    aggregated_L6_to_L5 = aggregated_L6_to_L5[['Level_5', 'Pressure', 'SubregionName', 'Sensitivity_x']]

    # Reset columns within aggregated_L6_to_L5 DF
    aggregated_L6_to_L5.columns = ['Level_5', 'Pressure', 'SubregionName', 'Sensitivity']

    # merge the information which is known in L5 and unknown in L6 into the original L5 without L6 data
    original_L5_without_L6_DF = original_L5_without_L6_DF.append(L6_Unknown_L5_known)
    # End of OG additions 09/22

    # APPEND TOGETHER aggregated_L6_to_L5 + original_L5_without_L6_DF
    aggregated_L6_to_L5 = aggregated_L6_to_L5.append(original_L5_without_L6_DF)

    # Fixing issues when there are the same pressures/subregions in the new additional data
    # to the existing data
    aggregated_L6_to_L5 = aggregated_L6_to_L5.drop_duplicates(['Level_5', 'Pressure', 'SubregionName'])

    # Apply the counter() function to the DF to count the occurrence of all assessment values
    aggregated_L6_to_L5[[
        'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
        'Unknown'
    ]] = aggregated_L6_to_L5.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later in code
    aggregated_L6_to_L5['Count_High'] = aggregated_L6_to_L5['High']
    aggregated_L6_to_L5['Count_Medium'] = aggregated_L6_to_L5['Medium']
    aggregated_L6_to_L5['Count_Low'] = aggregated_L6_to_L5['Low']
    aggregated_L6_to_L5['Count_NotSensitive'] = aggregated_L6_to_L5['Not sensitive']
    aggregated_L6_to_L5['Count_NotRel'] = aggregated_L6_to_L5['Not relevant']
    aggregated_L6_to_L5['Count_NoEvidence'] = aggregated_L6_to_L5['No evidence']
    aggregated_L6_to_L5['Count_NotAssessed'] = aggregated_L6_to_L5['Not assessed']
    aggregated_L6_to_L5['Count_Unknown'] = aggregated_L6_to_L5['Unknown']

    # Level 6 to 5 subsetting data where there are Level 5 assessments without L6 assessed values

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        aggregated_L6_to_L5[eachCol] = aggregated_L6_to_L5[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Level 6 to 5 aggregation (aggregation)

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_Sensitivity'] = \
        aggregated_L6_to_L5.apply(lambda df: create_sensitivity(df), axis=1)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_FinalSensitivity'] = \
        aggregated_L6_to_L5.apply(lambda df: final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_AssessedCount'] = \
        aggregated_L6_to_L5.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    aggregated_L6_to_L5['L5_UnassessedCount'] = \
        aggregated_L6_to_L5.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Apply column4() function to L6_processed DataFrame to create new Level_5 column
    aggregated_L6_to_L5['Level_4'] = aggregated_L6_to_L5.apply(lambda df: pd.Series(column_aggregate(df, 'Level_5')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    aggregated_L6_to_L5['L5_AggregationConfidenceValue'] = \
        aggregated_L6_to_L5.apply(lambda df: create_confidence(df), axis=1)

    # Drop unwanted data from L3_sens DataFrame
    L5_all = aggregated_L6_to_L5.drop([
        'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

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

    # Level 6 to 5 aggregation (creating an aggregated export)

    # Export aggregated data to be combined into the MasterFrame at the end of the script

    # Create DataFrame for master DataFrame at end of script
    L5_export = L5_all

    # Drop unwanted columns from L5_export DataFrame
    L5_export = L5_export.drop(['L5_Sensitivity', 'Sensitivity'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 5 to 4 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 5 to level 4

    # Group data by Level_4, Pressure, SubregionName and apply sensitivity values to list using lambdas function and
    # .apply() method
    L4_agg = L5_all.groupby(['Level_4', 'Pressure', 'SubregionName'
                             ])['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L4_agg = pd.DataFrame(L4_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L4_agg = L4_agg.reset_index(inplace=False)

    # Reset columns within L4_agg DataFrame
    L4_agg.columns = ['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']


    # Add new step to remove the A5.71 biotope from the L4_agg to ensure this is added back in as a new L4 biotope
    # which has not been aggregated
    L4_agg = L4_agg[L4_agg['Level_4'] != 'A5.71']

    ###########################
    # ADDING IN L4 DATA WHICH IS NOT SAMPLED FROM THE L6-L5 Aggregation
    ###########################

    # Extract all original level 5 data and assign to object oriented variable
    L4_maresa_insert = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['MHC_Level'].isin(['4'])])

     # OG Potential Fix

     # Assign data differences to new object oriented variable using outer merge between data frames
    assessed_L4L5_merge = pd.merge(original_L5_data, L4_maresa_insert, how='outer', on=['Level_4', 'Pressure','SubregionName'],indicator=True)

     # Create object for each unis level/subregion in L4 where there is no L5 - required for new edits added 27/02/2020
    assessed_L4_without_L5 = pd.DataFrame(assessed_L4L5_merge[assessed_L4L5_merge['Sensitivity_x'].isna()])
    assessed_L4_without_L5 = assessed_L4_without_L5[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity_y']]
    assessed_L4_without_L5.columns=['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']

    # OG 15/03/2023 fix so A5.71 is aggregating from level 4 even though A5.712 is in at level 5 - insert A5.71 back in here
    L4_maresa_insert_A5_71=L4_maresa_insert[L4_maresa_insert['EUNIS_Code']=='A5.71']
    L4_maresa_insert_A5_71 = L4_maresa_insert_A5_71[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']]

    assessed_L4_without_L5 = assessed_L4_without_L5.append(L4_maresa_insert_A5_71)
    # end of og fix a5.71

     # Subset by the L4_maresa_insert data which does not also appear within the L4 values created from 6 to 5
     # aggregations (L4_agg)
    L4_maresa_insert_without_aggregation = pd.merge(L4_maresa_insert, assessed_L4_without_L5, how='outer', on=['Level_4', 'Pressure','SubregionName'],indicator=True)
    L4_maresa_insert_without_aggregation = pd.DataFrame(L4_maresa_insert_without_aggregation[L4_maresa_insert_without_aggregation['_merge'] == 'both'])

     # Drop unwanted columns from 'original_L5_without_L6_DF' DataFrame
    L4_maresa_insert_without_aggregation = L4_maresa_insert_without_aggregation[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity_x']]
    L4_maresa_insert_without_aggregation.columns=['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']
     # End of OG Potential Fix


    # OG Changes 09/22 to allow for climate change pressures (if L5 unknown but L4 known then use L4 at L4)
    # create a new dataframe from which we will work out which L6 have all unknown values
    L5_Unknowns_Processing = L5_all

    L5_Unknowns_Processing = L5_Unknowns_Processing.groupby(['Level_4', 'Pressure', 'SubregionName']
                                                            )['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L5_Unknowns_Processing = pd.DataFrame(L5_Unknowns_Processing)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L5_Unknowns_Processing = L5_Unknowns_Processing.reset_index(inplace=False)

    #Create a subset of all rows that contain only unknowns
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Not sensitive") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Medium") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("No evidence") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Not relevant") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Not assessed") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Low") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("High") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("None") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Very low") == False]
    L5_Unknowns_Processing=L5_Unknowns_Processing[L5_Unknowns_Processing['Sensitivity'].str.contains("Very high") == False]
    # Drop unwanted columns from L5 unknown DataFrame
    L5_Unknowns_Processing = L5_Unknowns_Processing[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']]
    # create final L5 unknons dataset
    L5_unknowns = L5_Unknowns_Processing

    # Extract all original level 4 data and assign to object oriented variable
    original_L4_data = pd.DataFrame(bioreg_maresa_merge.loc[bioreg_maresa_merge['MHC_Level'].isin(['4'])])
  
    # Create a new datatfram where we will work out which L4 have known values
    L4_knowns_processing = original_L4_data
    # Drop unwanted columns
    L4_knowns_processing = L4_knowns_processing[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']]
    #Create a subset of all rows that contain only unknowns
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Not sensitive") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Medium") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("No evidence") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Not relevant") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Not assessed") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Low") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("High") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("None") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Very low") == False]
    L4_knowns_processing=L4_knowns_processing[L4_knowns_processing['Sensitivity'].str.contains("Very high") == False]
    # create final L4 unknons dataset
    L4_known_unknowns = L4_knowns_processing
    # Pull out all the L4 which arent in the unknown dataset - all known L4
    # Assign the unknown L4s and all the L4 data to new object oriented variable using outer merge between data frames
    original_L4_data_cleaned = L4_maresa_insert
    L4_known_and_unknown = pd.merge(original_L4_data_cleaned, L4_known_unknowns, how='outer', on=['Level_4', 'Pressure','SubregionName','Sensitivity'],indicator=True)
    # Create object for L4 where there are no unknowns
    L4_known = pd.DataFrame(L4_known_and_unknown[L4_known_and_unknown['_merge'] == 'left_only'])
    # Drop unwanted columns from L4 known DataFrame
    L4_known = L4_known[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']]

    # Work out which L5 unknowns are L4 knowns
    # Assign the unknown L5s and all the L5 data to new object oriented variable using outer merge between data frames
    L5_Unknown_L4_known_processing = pd.merge(L5_unknowns, L4_known, how='outer', on=['Level_4', 'Pressure','SubregionName'],indicator=True)
    # Pull out the data which are known for L5 and unknown for L6
    L5_Unknown_L4_known = pd.DataFrame(L5_Unknown_L4_known_processing[L5_Unknown_L4_known_processing['_merge'] == 'both'])
    # Drop unwanted columns
    L5_Unknown_L4_known = L5_Unknown_L4_known[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity_y']]
    # rename the columns
    L5_Unknown_L4_known.columns = ['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']

    # remove the columns from L5 aggregated to L4 dataset where they are unknown and L4 is known
    # Merge L5_Unknown_L4_known with the L4_agg table
    L4_agg_processing = pd.merge(L4_agg, L5_Unknown_L4_known, how='outer', on=['Level_4', 'Pressure','SubregionName'],indicator=True)
    # drop unknown rows fro L6 to L5 aggregated data
    L4_agg = pd.DataFrame(L4_agg_processing[L4_agg_processing['_merge'] == 'left_only'])
    L4_agg = L4_agg[['Level_4', 'Pressure', 'SubregionName', 'Sensitivity_x']]

    # Reset columns within aggregated_L6_to_L5 DF
    L4_agg.columns = ['Level_4', 'Pressure', 'SubregionName', 'Sensitivity']

    # merge the information which is known in L5 and unknown in L6 into the original L5 without L6 data
    L4_maresa_insert_without_aggregation = L4_maresa_insert_without_aggregation.append(L5_Unknown_L4_known)
    # End of OG additions 09/22



    # Append the data back into the L4_agg DF
    L4_agg = L4_agg.append(L4_maresa_insert_without_aggregation)

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L4_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = L4_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L4_agg['Count_High'] = L4_agg['High']
    L4_agg['Count_Medium'] = L4_agg['Medium']
    L4_agg['Count_Low'] = L4_agg['Low']
    L4_agg['Count_NotSensitive'] = L4_agg['Not sensitive']
    L4_agg['Count_NotRel'] = L4_agg['Not relevant']
    L4_agg['Count_NoEvidence'] = L4_agg['No evidence']
    L4_agg['Count_NotAssessed'] = L4_agg['Not assessed']
    L4_agg['Count_Unknown'] = L4_agg['Unknown']

    # Reassign L4_agg DataFrame to L4_sens for sensitivity aggregation
    L4_sens = L4_agg

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L4_sens[eachCol] = L4_sens[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Level 5 to 4 aggregation (aggregation)

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    L4_sens['L4_Sensitivity'] = L4_sens.apply(lambda df: create_sensitivity(df), axis=1)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    L4_sens['L4_FinalSensitivity'] = L4_sens.apply(lambda df: final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L4_sens['L4_AssessedCount'] = L4_sens.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L4_sens['L4_UnassessedCount'] = L4_sens.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Apply column3() function to L4_sens DataFrame to create new Level_3 column
    L4_sens['Level_3'] = L4_sens.apply(lambda df: pd.Series(column_aggregate(df, 'Level_4')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L4_sens['L4_AggregationConfidenceValue'] = L4_sens.apply(lambda df: create_confidence(df), axis=1)

    # Drop unwanted data from L3_sens DataFrame
    L4_sens = L4_sens.drop([
        'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 5 to 4 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L4_export = L4_sens

    # Drop unwanted columns from L4_export DataFrame
    L4_export = L4_export.drop(['Sensitivity', 'L4_Sensitivity'], axis=1, inplace=False)

    L4_export['Level_4'].unique()

    ####################################################################################################################

    # Level 4 to 3 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 4 to level 3

    # Group data by Level_3, Pressure, SubregionName and apply sensitivity values to list using lambdas function and
    # .apply() method
    L3_agg = L4_sens.groupby(['Level_3', 'Pressure', 'SubregionName'
                              ])['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L3_agg = pd.DataFrame(L3_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L3_agg = L3_agg.reset_index(inplace=False)

    # Reset columns within L3_agg DataFrame
    L3_agg.columns = ['Level_3', 'Pressure', 'SubregionName', 'Sensitivity']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L3_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = L3_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L3_agg['Count_High'] = L3_agg['High']
    L3_agg['Count_Medium'] = L3_agg['Medium']
    L3_agg['Count_Low'] = L3_agg['Low']
    L3_agg['Count_NotSensitive'] = L3_agg['Not sensitive']
    L3_agg['Count_NotRel'] = L3_agg['Not relevant']
    L3_agg['Count_NoEvidence'] = L3_agg['No evidence']
    L3_agg['Count_NotAssessed'] = L3_agg['Not assessed']
    L3_agg['Count_Unknown'] = L3_agg['Unknown']

    # Reassign L3_agg DataFrame to L3_sens for sensitivity aggregation
    L3_sens = L3_agg

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L3_sens[eachCol] = L3_sens[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Level 4 to 3 aggregation (aggregation)

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    L3_sens['L3_Sensitivity'] = L3_sens.apply(lambda df: create_sensitivity(df), axis=1)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    L3_sens['L3_FinalSensitivity'] = L3_sens.apply(lambda df: final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L3_sens['L3_AssessedCount'] = L3_sens.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L3_sens['L3_UnassessedCount'] = L3_sens.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Apply column2() function to L3_sens DataFrame to create new Level_2 column
    L3_sens['Level_2'] = L3_sens.apply(lambda df: pd.Series(column_aggregate(df, 'Level_3')), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L3_sens['L3_AggregationConfidenceValue'] = L3_sens.apply(lambda df: create_confidence(df), axis=1)

    # Drop unwanted data from L3_sens DataFrame
    L3_sens = L3_sens.drop([
        'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 4 to 3 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L3_export = L3_sens

    # Drop unwanted columns from L3_export DataFrame
    L3_export = L3_export.drop(['L3_Sensitivity', 'Sensitivity'], axis=1, inplace=False)

    ####################################################################################################################

    # Level 3 to 2 aggregation (formatting)

    # The following body of code begins the initial steps of the aggregation process from level 3 to level 2

    # Group data by Level_2, Pressure, SubregionName and apply sensitivity values to list using lambdas function and
    # .apply() method
    L2_agg = L3_sens.groupby(['Level_2', 'Pressure', 'SubregionName'
                              ])['Sensitivity'].apply(lambda x: ', '.join(x))

    # Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
    L2_agg = pd.DataFrame(L2_agg)

    # Reset index of newly created DataFrame to pull out data into 4 individual columns
    L2_agg = L2_agg.reset_index(inplace=False)

    # Reset columns within L2_agg DataFrame
    L2_agg.columns = ['Level_2', 'Pressure', 'SubregionName', 'Sensitivity']

    # Apply the counter() function to the DataFrame to count the occurrence of all assessment values
    L2_agg[['High', 'Medium', 'Low', 'Not sensitive', 'Not relevant', 'No evidence', 'Not assessed',
            'Unknown']] = L2_agg.apply(lambda df: pd.Series(counter(df['Sensitivity'])), axis=1)

    # Duplicate all count values and assign to new columns to be replaced by string values later
    L2_agg['Count_High'] = L2_agg['High']
    L2_agg['Count_Medium'] = L2_agg['Medium']
    L2_agg['Count_Low'] = L2_agg['Low']
    L2_agg['Count_NotSensitive'] = L2_agg['Not sensitive']
    L2_agg['Count_NotRel'] = L2_agg['Not relevant']
    L2_agg['Count_NoEvidence'] = L2_agg['No evidence']
    L2_agg['Count_NotAssessed'] = L2_agg['Not assessed']
    L2_agg['Count_Unknown'] = L2_agg['Unknown']

    # Reassign L2_agg DataFrame to L2_sens for sensitivity aggregation
    L2_sens = L2_agg

    # Run replacer() function on one set of newly duplicated columns to convert integers to string values of the
    # assessment score
    for eachCol in colNames:
        L2_sens[eachCol] = L2_sens[eachCol].apply(lambda x: replacer(x, eachCol))

    ####################################################################################################################

    # Level 3 to 2 aggregation (aggregation)

    # Use lambda function to apply create_sensitivity() function to each row within the DataFrame
    L2_sens['L2_Sensitivity'] = L2_sens.apply(lambda df: create_sensitivity(df), axis=1)

    # Use lambda function to apply final_sensitivity() function to each row within the DataFrame
    L2_sens['L2_FinalSensitivity'] = L2_sens.apply(lambda df: final_sensitivity(df), axis=1)

    # Use lambda function to apply combine_assessedcounts() function to each row within the DataFrame
    L2_sens['L2_AssessedCount'] = L2_sens.apply(lambda df: combine_assessedcounts(df), axis=1)

    # Use lambda function to apply combine_unassessedcounts() function to each row within the DataFrame
    L2_sens['L2_UnassessedCount'] = L2_sens.apply(lambda df: combine_unassessedcounts(df), axis=1)

    # Use lambda function to apply create_confidence() function to the DataFrame
    L2_sens['L2_AggregationConfidenceValue'] = L2_sens.apply(lambda df: create_confidence(df), axis=1)

    # Drop unwanted data from L2_sens DataFrame
    L2_sens = L2_sens.drop([
        'Sensitivity', 'Unknown', 'High', 'Medium', 'Low', 'Not sensitive', 'Not relevant',
        'No evidence', 'Not assessed', 'Count_High', 'Count_Medium', 'Count_Low',
        'Count_NotSensitive', 'Count_NotRel', 'Count_NoEvidence',
        'Count_NotAssessed', 'Count_Unknown'], axis=1, inplace=False)

    # Format columns into correct order
    L2_sens = L2_sens[['Level_2', 'Pressure', 'SubregionName', 'L2_FinalSensitivity', 'L2_Sensitivity',
                       'L2_AssessedCount', 'L2_UnassessedCount', 'L2_AggregationConfidenceValue']]

    ####################################################################################################################

    # Level 3 to 2 aggregation (creating an aggregated export)

    # Create DataFrame for master DataFrame at end of script
    L2_export = L2_sens

    # Drop unwanted columns from L2_export DataFrame
    L2_export = L2_export.drop(['L2_Sensitivity'], axis=1, inplace=False)

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
        lambda df: categorise_confidence(df, 'L6_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 5
    MasterFrame['L5_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: categorise_confidence(df, 'L5_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 4
    MasterFrame['L4_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: categorise_confidence(df, 'L4_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 3
    MasterFrame['L3_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: categorise_confidence(df, 'L3_AggregationConfidenceValue'), axis=1)

    # Create categories for confidence values: EUNIS Level 2
    MasterFrame['L2_AggregationConfidenceScore'] = MasterFrame.apply(
        lambda df: categorise_confidence(df, 'L2_AggregationConfidenceValue'), axis=1)

    ####################################################################################################################

    # Exporting the MasterFrame

    # Create correct order for columns within MasterFrame
    MasterFrame = MasterFrame[
        [
            'Pressure', 'SubregionName', 'Level_2', 'L2_FinalSensitivity', 'L2_AssessedCount', 'L2_UnassessedCount',
            'L2_AggregationConfidenceValue', 'L2_AggregationConfidenceScore', 'Level_3',
            'L3_FinalSensitivity', 'L3_AssessedCount', 'L3_UnassessedCount', 'L3_AggregationConfidenceValue',
            'L3_AggregationConfidenceScore', 'Level_4', 'L4_FinalSensitivity', 'L4_AssessedCount',
            'L4_UnassessedCount', 'L4_AggregationConfidenceValue', 'L4_AggregationConfidenceScore', 'Level_5',
            'L5_FinalSensitivity', 'L5_AssessedCount', 'L5_UnassessedCount', 'L5_AggregationConfidenceValue',
            'L5_AggregationConfidenceScore', 'Level_6', 'L6_FinalSensitivity', 'L6_AssessedCount', 'L6_UnassessedCount',
            'L6_AggregationConfidenceValue', 'L6_AggregationConfidenceScore'
        ]
    ]


    # All 'A2.611' values within the Level_5 column were found to be erroneous and need to be replaced with the string
    # value of 'Not Applicable'
    MasterFrame.loc[MasterFrame['Level_5'] == 'A2.611', 'L5_FinalSensitivity'] = 'Not Applicable'

    # All 'B3' values within the Level_2 column were found to be erroneous and need to be replaced with the string value
    # of 'Not Applicable'
    MasterFrame.loc[MasterFrame['Level_2'] == 'B3', 'L2_FinalSensitivity'] = 'Not Applicable'

    # Remove all A6 biotopes from the MasterFrame (temporary fix 01/07/2020)
    MasterFrame = MasterFrame[MasterFrame.Level_2 != 'A6']

    # Review the newly developed MasterFrame, and export to a .csv format file. To export the data, utilise the export
    # code which is stored as a comment (#) - ensure that you select an appropriate file path when completing this
    # stage.

    # Export MasterFrame in CSV format  - Offshore Only

    # Define folder file path to be saved into
    outpath = "./MarESA/Output/"+output_file
    # Define file name to save, categorised by date
    filename = "OffshoreSensAgg_" + (time.strftime("%Y%m%d") + "_" + str(bioreg_version) + '_' + str(maresa_version) +
                                     ".csv")
    # Run the output DF.to_csv method
    MasterFrame.to_csv(outpath + filename, sep=',')

    # Stop the timer post computation and print the elapsed time
    elapsed = (time.process_time() - start)

    # Create print statement to indicate how long the process took and round value to 1 decimal place.
    print('...The ' + str(filename) + ' script took ' +
        str(round(elapsed / 60, 1)) + ' minutes to run and complete.' +
        '\n' + 'This has been saved as a time-stamped output at ' +
        'the following filepath: ' + str(outpath) + '\n\n')



if __name__ == "__main__":
    os.chdir('C://Users//Ollie.Grint//Documents/projects/marine-sensitivity-aggregations')
    main('MarESA-Data-Extract-habitatspressures_2023_11_07.csv', 'BioregionsExtract_20220310.xlsx','code switch test/')

