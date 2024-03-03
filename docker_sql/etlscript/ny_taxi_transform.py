# from ny_taxi_extract import download_store_data
import pandas as pd

def join_pick_drop(column_a, column_b):
    return str(column_a).strip() + "_" + str(column_b).strip()

def get_date_info(df,column:str,new_column_prefix:str):
        # df = df.copy()
        df[f'{new_column_prefix}_year'] = df[column].dt.year
        df[f'{new_column_prefix}_month'] = df[column].dt.month
        df[f'{new_column_prefix}_day'] = df[column].dt.day
        df[f'{new_column_prefix}_hour'] = df[column].dt.hour
        df[f'{new_column_prefix}_minute'] = df[column].dt.minute
        df[f'{new_column_prefix}_day_of_week'] = df[column].dt.day_of_week
        df[f'{new_column_prefix}_day_name'] = df[column].dt.day_name()
        df[f'{new_column_prefix}_is_month_end'] = df[column].dt.is_month_end
        df[f'{new_column_prefix}_is_year_end'] = df[column].dt.is_year_end
        

        return df

def transform_data(data:pd.DataFrame):
    lower_column_name = [col.lower() for col in data.columns]
    data.columns = lower_column_name
    fact_columns = ['tpep_dropoff_datetime','tpep_pickup_datetime','passenger_count', 'trip_distance','fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount', 'congestion_surcharge', 'airport_fee']

    dimension_table_data = ['tpep_dropoff_datetime','tpep_pickup_datetime','ratecodeid','store_and_fwd_flag','pulocationid','dolocationid','payment_type']

    dimension_table_data = data[dimension_table_data]

    time_table_data = data[['tpep_dropoff_datetime','tpep_pickup_datetime']]

    

    df_time_table_pickup = get_date_info(time_table_data,'tpep_dropoff_datetime','pickup')

    df_time_table_dropoff = get_date_info(df_time_table_pickup,'tpep_dropoff_datetime','dropoff')


    df_time_table_dropoff['datetime_id'] = df_time_table_dropoff.apply(lambda x: join_pick_drop(x['tpep_pickup_datetime'], x['tpep_dropoff_datetime']), axis=1)

    fact_data_table = data[fact_columns]

    fact_data_table['datetime_id'] = fact_data_table.apply(lambda x: join_pick_drop(x['tpep_pickup_datetime'], x['tpep_dropoff_datetime']), axis=1)

    fact_data_table.drop(['tpep_dropoff_datetime','tpep_pickup_datetime'],axis=1,inplace=True)

    fact_data_table = fact_data_table[fact_data_table.columns[::-1]]

    dimension_table_data['datetime_id'] = dimension_table_data.apply(lambda x: join_pick_drop(x['tpep_pickup_datetime'], x['tpep_dropoff_datetime']), axis=1)

    dimension_table_data.drop(['tpep_dropoff_datetime','tpep_pickup_datetime'],axis=1,inplace=True)

    ordered_column_time = ['datetime_id','tpep_dropoff_datetime',
    'tpep_pickup_datetime',
    'pickup_year',
    'pickup_month',
    'pickup_day',
    'pickup_hour',
    'pickup_minute',
    'pickup_day_of_week',
    'pickup_day_name',
    'pickup_is_month_end',
    'pickup_is_year_end',
    'dropoff_year',
    'dropoff_month',
    'dropoff_day',
    'dropoff_hour',
    'dropoff_minute',
    'dropoff_day_of_week',
    'dropoff_day_name',
    'dropoff_is_month_end',
    'dropoff_is_year_end']

    df_time_table_dropoff = df_time_table_dropoff[ordered_column_time]

    df_time_table_dropoff.tpep_dropoff_datetime = df_time_table_dropoff.tpep_dropoff_datetime.dt.to_pydatetime()

    df_time_table_dropoff.tpep_pickup_datetime = df_time_table_dropoff.tpep_pickup_datetime.dt.to_pydatetime()

    df_time_table_dropoff.drop_duplicates(subset=['datetime_id'], inplace=True)

    df_time_table_dropoff.reset_index(drop=True, inplace=True)
    fact_data_table.drop_duplicates(subset=['datetime_id'], inplace=True)
    fact_data_table.reset_index(drop=True, inplace=True)

    dimension_table_data.drop_duplicates(subset=['datetime_id'], inplace=True)

    dimension_table_data.reset_index(drop=True, inplace=True)

    dimension_table_data = dimension_table_data[['datetime_id','ratecodeid','store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type']]

    return dimension_table_data, df_time_table_dropoff, fact_data_table