import pandas as pd

#Data shifter for selected columns in DF
def _shift_data_in_columns(dt_source:pd.DataFrame, lst_columns, n_shift=1):
    temp = dt_source.copy()
    for column in lst_columns:
        temp[column + "_shifted["+str(n_shift)+"]"] = temp[column].shift(n_shift)
    return temp