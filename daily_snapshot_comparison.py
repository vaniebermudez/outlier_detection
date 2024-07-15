from sqlalchemy import create_engine
import os
import datetime
from collections import Counter
import pandas as pd
import numpy as np
import glob
from datetime import datetime


def date_time_now():
    """Set Report Date"""

    now = datetime.now() #- timedelta(days = 7)
    report_date = now.strftime('%Y-%m-%d')
    return report_date


def combined_csv(folder, file_suffix, limit_file=8):
    """Combines X number of csv files"""

    csv_files = glob.glob(folder + "/*_"+ file_suffix +".csv")
    csv_files = sorted(csv_files, reverse=True)
    csv_files = csv_files[:limit_file]

    combined_df = pd.DataFrame()

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        combined_df = pd.concat([combined_df, df])
    
    return combined_df


def group_data(df, field_list, value_list):
    """Group df using field_list aggregated by value_list"""

    df_grouped = df[field_list + value_list]
    df_grouped = df_grouped.groupby(by=field_list).sum(value_list).reset_index()
    return df_grouped


def merge_data(df1, df2, left_values, right_values, how_mech='outer'):
    """Merge data - for RDO vs HFM"""

    df_merged = pd.merge(df1, df2, 
             left_on=left_values,
             right_on=right_values,
             how=how_mech)
    return df_merged


def trend_data(df, field_list, value_list, extract_date, aggregation="sum"):
    """Get trended data for Daily Snapshot comparison"""

    df_trended = pd.pivot_table(df, values=value_list, index=field_list,
                           columns=extract_date, aggfunc=aggregation, fill_value=0,).reset_index()
    return df_trended


def time_frame(df, time_frame, last_year_month):
    """Is it FY, PFY, YTD, MTD"""
    new_df = pd.DataFrame()
    
    if time_frame == 'No Filter':
        return df
    elif time_frame == 'FY':
        year = int(last_year_month[:4])
        start_month = 1
        end_month = 12
    elif time_frame == 'PFY':
        year = int(last_year_month[:4]) - 1
        start_month = 1
        end_month = 12
    elif time_frame == 'YTD':
        year = int(last_year_month[:4])
        start_month = 1
        end_month = int(last_year_month[5:])
    elif time_frame == 'MTD':
        year = int(last_year_month[:4])
        start_month = int(last_year_month[5:])
        end_month = int(last_year_month[5:])
    else:
        print('Input correct time frame')
    
    new_df = df[(df['year'] == year)]
    new_df = new_df[new_df['month'] >= start_month]
    new_df = new_df[new_df['month'] <= end_month]

    return new_df


def gap(df, column_count):
    """Gap from last x columns"""

    last_x_columns = df.iloc[:, -column_count:]

    # Calculate the absolute gap
    absolute_gaps = last_x_columns - last_x_columns.shift(1, axis=1)
    absolute_gaps.columns = [f'{col}_gap' for col in last_x_columns.columns]

    # Calculate the percentage gap
    percentage_gaps = ((last_x_columns - last_x_columns.shift(1, axis=1)) / last_x_columns.shift(1, axis=1))
    percentage_gaps.columns = [f'{col}_pct_gap' for col in last_x_columns.columns]

    # Concatenate the results with the original dataframe
    result_df = pd.concat([df, absolute_gaps, percentage_gaps], axis=1)
    result_df.fillna(0)

    return result_df


def flag_outliers(prev_df, df, column_count, gap_upper, gap_lower, pct_gap_upper, pct_gap_lower):
    """Flagging of outliers from set limits"""

    cols_to_check = prev_df.columns[-column_count:].tolist()

    for col in cols_to_check:
        gap_col = f'{col}_gap'
        pct_gap_col = f'{col}_pct_gap'

        df['{}_flag'.format(col)] = (((df[gap_col] > gap_upper) | (df[gap_col] < gap_lower)) & 
                                     ((df[pct_gap_col] > pct_gap_upper) | (df[pct_gap_col] < pct_gap_lower)))

    return df


def summarize_outliers(prev_df, df, time_frame, field_list,  column_count, top_count):
    """Create Summary report of outliers"""

    # flag_cols = df.filter(regex='flag$')

    cols_to_check = prev_df.columns[-column_count:].tolist()

    summary_cols = ['attribute_label','attribute_value', 'tag',  'time_frame','date_prev','date_curr', 'amt_prev', 'amt_curr', 'gap', 'pct_gap']

    summary_df = pd.DataFrame(columns=summary_cols)
    temp_df = pd.DataFrame()

    for col in cols_to_check[1:]:
        # i = 0
        sorted_df_desc = df.sort_values(by=[f'{col}_gap', f'{col}_pct_gap'], ascending=[False, False]).reset_index()
        filter_df_desc = sorted_df_desc.head(top_count)

        for column_name, row in filter_df_desc.iteritems():
            # i= i + 1  
            # temp_df['top'] = i
            temp_df['tag'] = 'increase'
            temp_df['time_frame'] = time_frame
            temp_df['attribute_label'] = column_name[0]
            temp_df['attribute_value'] = row
            temp_df['date_prev'] = None
            temp_df['date_curr'] = col[1] 
            temp_df['amt_prev'] = None
            temp_df['amt_curr'] = filter_df_desc[col]
            temp_df['gap'] = filter_df_desc[f'{col}_gap'] 
            temp_df['pct_gap'] = filter_df_desc[f'{col}_pct_gap']

            summary_df = summary_df.append(temp_df)

        sorted_df_asc = df.sort_values(by=[f'{col}_gap', f'{col}_pct_gap'], ascending=[True, True]).reset_index()
        filter_df_asc = sorted_df_asc.head(top_count)

        for column_name, row in filter_df_asc.iteritems():
            # i= i + 1  
            # temp_df['top'] = i
            temp_df['tag'] = 'decrease'
            temp_df['time_frame'] = time_frame
            temp_df['attribute_label'] = column_name[0]
            temp_df['attribute_value'] = row
            temp_df['date_prev'] = None
            temp_df['date_curr'] = col[1] 
            temp_df['amt_prev'] = None
            temp_df['amt_curr'] = filter_df_asc[col]
            temp_df['gap'] = filter_df_asc[f'{col}_gap'] 
            temp_df['pct_gap'] = filter_df_asc[f'{col}_pct_gap']

            summary_df = summary_df.append(temp_df)

    summary_df = summary_df.reset_index()

    summary_df = summary_df[(((summary_df['gap'] > 0) & (summary_df['tag'] == 'increase')) | 
                                     ((summary_df['gap'] < 0) & (summary_df['tag'] == 'decrease')))]
    return summary_df
            








def write_to_excel(filename,sheetname,dataframe):
    """Write to existing excel"""
    
    with pd.ExcelWriter(filename, engine="openpyxl", mode='a', if_sheet_exists="replace") as writer: 
        dataframe.to_excel(writer, sheet_name=sheetname,index=False)

