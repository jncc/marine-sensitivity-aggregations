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
    with open('./Outputs/bioregions_comparison.txt', 'w') as f:
        f.write('####### Changes #######\n\n')
    for row in dfDiff.index:
        if (row in df_old.index) and (row in df_new.index):
            for col in sharedCols:
                value_old = df_old.loc[row,col]
                value_new = df_new.loc[row,col]
                if value_old==value_new:
                    dfDiff.loc[row,col] = df_new.loc[row,col]
                else:
                    dfDiff.loc[row,col] = ('{}->{}').format(value_old,value_new)
                    print('Row - ', row, '\nChange - ', dfDiff.loc[row,col], '\n')
                    with open('./Outputs/bioregions_comparison.txt', 'a') as f:
                        f.write('Row - ' + row + '\n')
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

    with open('./Outputs/bioregions_comparison.txt', 'a') as f:
        f.write('\n####### New and deleted lines #######\n')
        f.write('\n' + str(len(newRows)) + ' New Rows:\n\n')
        for item in newRows:
            f.write(f"{item}\n")
        f.write('\n' + str(len(droppedRows)) + ' Dropped Rows:\n\n')
        for item in droppedRows:
            f.write(f"{item}\n")
    print('\nDone.\n')


def main():
    path_OLD = Path('./Data/BioregionsExtract_20201105.xlsx')
    path_NEW = Path('./Data/BioregionsExtract_20210802.xlsx')

    df_old = pd.read_excel(path_OLD).fillna('')
    df_new = pd.read_excel(path_NEW).fillna('')

    df_old = df_old.sort_values(['HabitatCode', 'SubregionName'])
    df_new = df_new.sort_values(['HabitatCode', 'SubregionName'])

    df_old['ID'] = df_old['HabitatCode'] + ' - ' + df_old['SubregionName']
    df_new['ID'] = df_new['HabitatCode'] + ' - ' + df_new['SubregionName']

    df_old = df_old.drop('Unnamed: 0', axis=1).drop_duplicates()
    df_new = df_new.drop('Unnamed: 0', axis=1).drop_duplicates()

    df_old = df_old.set_index('ID')
    df_new = df_new.set_index('ID')

    excel_diff(df_old, df_new)


if __name__ == '__main__':
    main()
