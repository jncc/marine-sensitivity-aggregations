# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 12:44:23 2023

@author: Ollie.Grint
"""

import os
import pandas as pd
from pathlib import Path
import re
from collections import Counter

########################################################################################################################################################

# Set the file the outputs were written into
path = 'C:/Users/Ollie.Grint/Documents/marine-sensitivity-aggregations/MarESA/Output/20231107'

Sens_file="OffshoreSensAgg_20231107_Bioreg20220310_marESA07.csv"
Res_file="OffshoreResAgg_20231107_Bioreg20220310_marESA07.csv"
Resil_file="OffshoreResilAgg_20231107_Bioreg20220310_marESA07.csv"

climate = ['Global warming (Extreme)', 'Global warming (High)', 'Global warming (Middle)',
                   'Ocean Acidification (High)', 'Ocean Acidification (Middle)', 'Sea level rise (Extreme)',
                   'Sea level rise (High)', 'Sea level rise (Middle)', 'Marine heatwaves (High)', 
                   'Marine heatwaves (Middle)']

########################################################################################################################################################

os.chdir(path)

Path("Duplicates_check").mkdir(parents=True, exist_ok=True)
Path("Aggregation_check").mkdir(parents=True, exist_ok=True)


########################################################################################################################################################

# Check for duplicates for sens
Sens = pd.read_csv(Sens_file, index_col=0)

# check L6 duplicates where L6 not na
Sens_L6=Sens[Sens['Level_6'].notnull()]
Sens_L6['duplicated'] = Sens_L6.duplicated(subset=['Pressure','Level_6','SubregionName'])
Sens_L6_Duplicated = Sens_L6[Sens_L6['duplicated']==True]
Sens_L6_Duplicated = Sens_L6_Duplicated.drop('duplicated', axis=1)
Sens_L6 = Sens_L6.drop('duplicated', axis=1)

# Export any duplicates and print the number
Sens_L6.to_csv('Duplicates_check/'+Sens_file, index=False)
Sens_duplicates_len=len(Sens_L6)
print(f'number of duplicates in OffshoreSensAgg: {Sens_duplicates_len}')



########################################################################################################################################################

def check_aggregations(Level_df,Level_no_aggregated_from):
    
    #Level_df=Sens
    #Level_no_aggregated_from='4'
    
    Level_aggregated_to_str=str(int(Level_no_aggregated_from)-1)
    
    Not_aggregated_correctly_cut=pd.DataFrame()
    
    Level_aggregated_from='Level_'+Level_no_aggregated_from
    Level_aggregated_from_AssesedCount='L'+Level_no_aggregated_from+'_AssessedCount'
    Level_aggregated_from_UnAssesedCount='L'+Level_no_aggregated_from+'_UnassessedCount'
    Level_aggregated_to='Level_'+Level_aggregated_to_str
    Level_aggregated_to_AssesedCount='L'+Level_aggregated_to_str+'_AssessedCount'
    Level_aggregated_to_UnassesedCount='L'+Level_aggregated_to_str+'_UnassessedCount'

    # get the list of unique combinations of L5 
    unique = Level_df.drop_duplicates(['Pressure',Level_aggregated_to,'SubregionName']).reset_index()

    # loop through these unique
    for n in range(0,len(unique)):
        entry=unique[Level_aggregated_to][n]

        pressure=unique['Pressure'][n]
        
        SubregionName=unique['SubregionName'][n]

        entry_prep=Level_df[Level_df[Level_aggregated_to]==entry].reset_index(drop=True)
        entry_prep=entry_prep[entry_prep['Pressure']==pressure]
        entry_prep=entry_prep[entry_prep['SubregionName']==SubregionName].reset_index(drop=True)
        
        if len(entry_prep) ==0:
            pass
        else:
            print(entry_prep['Pressure'])
            print(entry_prep[Level_aggregated_to_AssesedCount][0])
            print(type(entry_prep[Level_aggregated_to_AssesedCount][0]))
            assesed=entry_prep[Level_aggregated_to_AssesedCount][0]
            unassessed=entry_prep[Level_aggregated_to_UnassesedCount][0]
            

        # Pull out the entries from the level above that were aggregated down
            level_above_entries_prep=Level_df[Level_df[Level_aggregated_to]==entry].reset_index(drop=True)
            level_above_entries_prep=level_above_entries_prep[level_above_entries_prep['Pressure']==pressure]
            level_above_entries_prep=level_above_entries_prep[level_above_entries_prep['SubregionName']==SubregionName].reset_index(drop=True)
            level_above_entries = level_above_entries_prep.drop_duplicates(['Pressure',Level_aggregated_from,'SubregionName']).reset_index()
            
            # pull out the assessed and unassesed counts
            level_above_entries_assesed=list(level_above_entries[Level_aggregated_from_AssesedCount])
            level_above_entries_unassesed=list(level_above_entries[Level_aggregated_from_UnAssesedCount])

            # Check if Level above is nan for both
            if str(level_above_entries_assesed) =='[nan]' and str(level_above_entries_unassesed) =='[nan]':
                pass
            
            # check if level is nan but level above isnt
            elif str(assesed)=='nan' and str(unassessed)=='nan':

                Not_aggregated_correctly_cut=Not_aggregated_correctly_cut.append(level_above_entries_prep, ignore_index=True) 
                
                
            # check if all unassesed and assesed are non applicable
            elif all(x == 'Not Applicable' for x in level_above_entries_assesed ) and all(x == 'Not Applicable' for x in level_above_entries_unassesed ):

                Not_aggregated_correctly_cut=Not_aggregated_correctly_cut.append(level_above_entries_prep, ignore_index=True) 

                
            else:


                # Check the assessed aggregation
                if not all(str(x) in ['Not Applicable','nan'] for x in level_above_entries_assesed ):
                    print(assesed)
                    # Create a list of all the assesed scores
                    assesed_list=assesed.split(",")
                    assesed_counts=[]
                    for i in assesed_list:
                        i=i.strip()
                        measure=i.split('(')[0]
                        if measure=='Not Applicable':
                            assesed_counts.append(measure)
                        else:
                            occurances=re.findall(r'\((.+)\)', i)
                            occurances=int(occurances[0])
                            for i in range(0,occurances):
                                assesed_counts.append(measure)

                    assesed_counts.sort()
                    
                    level_above_assesed=[]
                    for x in level_above_entries_assesed:
                        if x =='Not Applicable':
                            pass
                        elif str(x) =='nan':
                            pass
                        else:
                        
                            level_above_assesed_list=x.split(",")
                            for i in level_above_assesed_list:
                                i=i.strip()
                                level_above_measure=i.split('(')[0]
                                level_above_occurances=re.findall(r'\((.+)\)', i)
                                level_above_occurances=int(level_above_occurances[0])
                                for i in range(0,level_above_occurances):
                                    level_above_assesed.append(level_above_measure)
                    
                    level_above_assesed.sort()
                    
                    if level_above_assesed != assesed_counts:
                        # check if the rule for climate aggregations is in place (if L6 unknown and L5 known use L5) 
                        if pressure in climate:
                            if level_above_entries_assesed[0] =='Not Applicable' and level_above_entries_unassesed[0] =='Unknown' and any(x in assesed_counts for x in ['NS','NE','L','M','H','NR','NA']):
                                pass
                            else:
                                Not_aggregated_correctly_cut=Not_aggregated_correctly_cut.append(level_above_entries_prep, ignore_index=True) 


                # Check the unassessed aggregation
                if not all(str(x) in ['Not Applicable','nan'] for x in level_above_entries_unassesed ):

                    # Create a list of all the unassesed scores
                    unassessed_list=unassessed.split(",")
                    unassesed_counts=[]
                    for i in unassessed_list:
                        i=i.strip()
                        measure=i.split('(')[0]
                        if measure=='Not Applicable':
                            unassesed_counts.append(measure)
                        else:
                            occurances=re.findall(r'\((.+)\)', i)
                            occurances=int(occurances[0])
                            for i in range(0,occurances):
                                unassesed_counts.append(measure)
                        
                    unassesed_counts.sort()
                    
                    level_above_unassesed=[]
                    for x in level_above_entries_unassesed:
                        if x =='Not Applicable':
                            pass
                        elif str(x) =='nan':
                            pass
                        else:
        
                            level_above_unassesed_list=x.split(",")
                            for i in level_above_unassesed_list:
                                i=i.strip()
                                level_above_measure=i.split('(')[0]
                                level_above_occurances=re.findall(r'\((.+)\)', i)
                                level_above_occurances=int(level_above_occurances[0])
                                for i in range(0,level_above_occurances):
                                    level_above_unassesed.append(level_above_measure)
                    
                    level_above_unassesed.sort()
                    
                    if level_above_unassesed != unassesed_counts:
                        # check if the rule for climate aggregations is in place (if L6 unknown and L5 known use L5) 
                        if pressure in climate:
                            if level_above_entries_unassesed[0].split("(")[0] =='UN' and any(x in assesed_counts for x in ['NS','NE','L','M','H','NR','NA']):
                                pass
                            else:

                                Not_aggregated_correctly_cut=Not_aggregated_correctly_cut.append(level_above_entries_prep, ignore_index=True) 


    # print the number that failed at this step
    Not_aggregated_correctly_len=len(Not_aggregated_correctly_cut)
    print(f'number of aggregations that failed at L{Level_no_aggregated_from} -> L{Level_aggregated_to} in OffshoreSensAgg: {Not_aggregated_correctly_len}')

    return(Not_aggregated_correctly_cut)



########################################################################################################################################################
# create empty dataframe to write into
Not_aggregated_correctly_Sens=pd.DataFrame()

# check L5-L6 aggregation
L6_L5_aggregation_failures_Sens=check_aggregations(Sens_L6,'6')
L5_L4_aggregation_failures_Sens=check_aggregations(Sens,'5')
L4_L3_aggregation_failures_Sens=check_aggregations(Sens,'4')
L3_L2_aggregation_failures_Sens=check_aggregations(Sens,'3')

Not_aggregated_correctly_Sens=Not_aggregated_correctly_Sens.append(L6_L5_aggregation_failures_Sens, ignore_index=True) 
Not_aggregated_correctly_Sens=Not_aggregated_correctly_Sens.append(L5_L4_aggregation_failures_Sens, ignore_index=True) 
Not_aggregated_correctly_Sens=Not_aggregated_correctly_Sens.append(L4_L3_aggregation_failures_Sens, ignore_index=True) 
Not_aggregated_correctly_Sens=Not_aggregated_correctly_Sens.append(L3_L2_aggregation_failures_Sens, ignore_index=True) 

# Export any duplicates and print the number
Not_aggregated_correctly_Sens.to_csv('Aggregation_check/'+Sens_file, index=False)

Not_aggregated_correctly_Sens_len=len(Not_aggregated_correctly_Sens)
print(f'number of aggregation failures in OffshoreSensAgg: {Not_aggregated_correctly_Sens_len}')

########################################################################################################################################################

# Check for duplicates
Res = pd.read_csv(Res_file, index_col=0)

Res_L6=Res[Res['Level_6'].notnull()]
Res_L6['duplicated'] = Res_L6.duplicated(subset=['Pressure','Level_6','SubregionName'])
Res_L6_Duplicated = Res_L6[Res_L6['duplicated']==True]
Res_L6_Duplicated = Res_L6_Duplicated.drop('duplicated', axis=1)
Res_L6 = Res_L6.drop('duplicated', axis=1)

Res_L6.to_csv('Duplicates_check/'+Res_file, index=False)

Res_duplicates_len=len(Res_L6)
print(f'number of duplicates in OffshoreSensAgg: {Res_duplicates_len}')

########################################################################################################################################################
# create empty dataframe to write into
Not_aggregated_correctly_Res=pd.DataFrame()

Agg_check_L6=Res_L6[Res_L6['L6_FinalResistance']!= 'None']

# check L5-L6 aggregation
L6_L5_aggregation_failures_Res=check_aggregations(Agg_check_L6,'6')
L5_L4_aggregation_failures_Res=check_aggregations(Res,'5')
L4_L3_aggregation_failures_Res=check_aggregations(Res,'4')
L3_L2_aggregation_failures_Res=check_aggregations(Res,'3')

Not_aggregated_correctly_Res=Not_aggregated_correctly_Res.append(L6_L5_aggregation_failures_Res, ignore_index=True) 
Not_aggregated_correctly_Res=Not_aggregated_correctly_Res.append(L5_L4_aggregation_failures_Res, ignore_index=True) 
Not_aggregated_correctly_Res=Not_aggregated_correctly_Res.append(L4_L3_aggregation_failures_Res, ignore_index=True) 
Not_aggregated_correctly_Res=Not_aggregated_correctly_Res.append(L3_L2_aggregation_failures_Res, ignore_index=True) 

# Export any duplicates and print the number
Not_aggregated_correctly_Res.to_csv('Aggregation_check/'+Res_file, index=False)

Not_aggregated_correctly_Res_len=len(Not_aggregated_correctly_Res)
print(f'number of aggregation failures in OffshoreSensAgg: {Not_aggregated_correctly_Res_len}')

########################################################################################################################################################

# Check for duplicates
Resil = pd.read_csv(Resil_file, index_col=0)

Resil_L6=Resil[Resil['Level_6'].notnull()]
Resil_L6['duplicated'] = Resil_L6.duplicated(subset=['Pressure','Level_6','SubregionName'])
Resil_L6_Duplicated = Resil_L6[Resil_L6['duplicated']==True]
Resil_L6_Duplicated = Resil_L6_Duplicated.drop('duplicated', axis=1)
Resil_L6 = Resil_L6.drop('duplicated', axis=1)

Resil_L6.to_csv('Duplicates_check/'+Resil_file, index=False)

Resil_duplicates_len=len(Resil_L6)
print(f'number of duplicates in OffshoResilensAgg: {Resil_duplicates_len}')


########################################################################################################################################################
# create empty dataframe to write into
Not_aggregated_correctly_Resil=pd.DataFrame()


# check L5-L6 aggregation
L6_L5_aggregation_failures_Resil=check_aggregations(Resil_L6,'6')
L5_L4_aggregation_failures_Resil=check_aggregations(Resil,'5')
L4_L3_aggregation_failures_Resil=check_aggregations(Resil,'4')
L3_L2_aggregation_failures_Resil=check_aggregations(Resil,'3')


Not_aggregated_correctly_Resil=Not_aggregated_correctly_Resil.append(L6_L5_aggregation_failures_Resil, ignore_index=True) 
Not_aggregated_correctly_Resil=Not_aggregated_correctly_Resil.append(L5_L4_aggregation_failures_Resil, ignore_index=True) 
Not_aggregated_correctly_Resil=Not_aggregated_correctly_Resil.append(L4_L3_aggregation_failures_Resil, ignore_index=True) 
Not_aggregated_correctly_Resil=Not_aggregated_correctly_Resil.append(L3_L2_aggregation_failures_Resil, ignore_index=True) 

# Export any duplicates and print the number
Not_aggregated_correctly_Resil.to_csv('Aggregation_check/'+Resil_file, index=False)

Not_aggregated_correctly_Resil_len=len(Not_aggregated_correctly_Resil)
print(f'number of aggregation failures in OffshoreSensAgg: {Not_aggregated_correctly_Resil_len}')














