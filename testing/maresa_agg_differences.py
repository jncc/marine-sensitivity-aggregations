import pandas as pd
from pathlib import Path



def excel_diff(df_old, df_new):

    dfDiff = df_new.copy()
    droppedRows = []
    newRows = []

    cols_old = df_old.columns
    cols_new = df_new.columns
    sharedCols = list(set(cols_old).intersection(cols_new))

    print('\n\nLooking for changes....\n')
    with open('./Outputs/maresa_agg_sens_comparison.txt', 'w') as f:
        f.write('####### Changes #######\n\n')
    for row in dfDiff.index:
        if (row in df_old.index) and (row in df_new.index):
            for col in sharedCols:
                value_old = df_old.loc[row,col]
                value_new = df_new.loc[row,col]
                if value_old==value_new:
                    dfDiff.loc[row,col] = df_new.loc[row,col]
                else:

                    #if ',' in value_old & value_new:

                        #test_old = value_old.replace(" ", "")
                        #test_new = value_new.replace(" ", "")
                    test_old = str(value_old).split(', ')
                    test_new = str(value_new).split(', ')
                    if set(test_old) == set(test_new):
                        dfDiff.loc[row,col] = df_new.loc[row,col]
                    else:
                        dfDiff.loc[row,col] = ('{}->{}').format(value_old,value_new)
                        print('Row - ', row, '\nChange - ', dfDiff.loc[row,col], '\n')
                        with open('./Outputs/maresa_agg_sens_comparison.txt', 'a') as f:
                            f.write('Row - ' + row + '\n')
                            f.write('Col - ' + col + '\n')
                            f.write('Change - ' + dfDiff.loc[row,col] + '\n\n')
        else:
            newRows.append(row)

    for row in df_old.index:
        if row not in df_new.index:
            droppedRows.append(row)
            dfDiff = dfDiff.append(df_old.loc[row,:])

    dfDiff = dfDiff.sort_index().fillna('')
    print('\nLooking for new or deleted rows....\n')
    print('\n' + str(len(newRows)) + f' New Rows: {newRows}')
    print('\n' + str(len(droppedRows)) + f' Dropped Rows: {droppedRows}')

    with open('./Outputs/maresa_agg_sens_comparison.txt', 'a') as f:
        f.write('\n####### New and deleted lines #######\n')
        f.write('\n' + str(len(newRows)) + ' New Rows:\n\n')
        for item in newRows:
            f.write(f"{item}\n")
        f.write('\n' + str(len(droppedRows)) + ' Dropped Rows:\n\n')
        for item in droppedRows:
            f.write(f"{item}\n")

    print('\nDone.\n')


def main():
    path_OLD = Path('./Data/OffshoreSensAgg_20210113_Bioreg20201105_MarESA20201112.csv')
    path_NEW = Path('./Data/OffshoreSensAgg_20210802_Bioreg20210802_marESA20210702.csv')

    df_old = pd.read_csv(path_OLD).fillna('')
    df_new = pd.read_csv(path_NEW).fillna('')

    #df_old = df_old.sort_values(['HabitatCode', 'SubregionName'])
    #df_new = df_new.sort_values(['HabitatCode', 'SubregionName'])

    df_old['ID'] = 'Hab-'+df_old['Level_2']+'-'+df_old['Level_3']+'-'+df_old['Level_4']+'-'+df_old['Level_5']+'-'+df_old['Level_6']+'-'+df_old['SubregionName']+'-'+df_old['Pressure']
    df_new['ID'] = 'Hab-'+df_new['Level_2']+'-'+df_new['Level_3']+'-'+df_new['Level_4']+'-'+df_new['Level_5']+'-'+df_new['Level_6']+'-'+df_new['SubregionName']+'-'+df_new['Pressure']

    df_old = df_old.drop('Unnamed: 0', axis=1).drop_duplicates(subset=['ID'])
    df_new = df_new.drop('Unnamed: 0', axis=1).drop_duplicates(subset=['ID'])

    df_old = df_old.sort_values('ID').set_index('ID')
    df_new = df_new.sort_values('ID').set_index('ID')

    #print(df_old.tail(50))

    excel_diff(df_old, df_new)


if __name__ == '__main__':
    main()
