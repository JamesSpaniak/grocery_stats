import dask
dask.config.set({'dataframe.query-planning': True})

import csv, random, sys, time
import pandas as pd
from sqlalchemy import create_engine

import dask.dataframe as dd
from dask.distributed import Client

from util import timer, read_file, compute_linear_regression, load_csv_into_db

FILE_NAME = "data/train_combined_14_n_16.csv"
PG_URI = 'postgresql://postgres:postgres@localhost:5432/postgres'

@timer
def process_data(file_name, client):
    ddf = read_file(file_name, use_dask=True, print_cols=True)
    df_cols = ['Transactions', 'Unit Sales']
    agg_fns = ['mean', 'std', 'min', 'max', 'count']
    
    ddf_gb = ddf.groupby(["State", "Item Nbr"]) # Lazy compute
    #client.persist(ddf_gb) # cache

    for col_name in df_cols:
        for agg in agg_fns: # run multiple aggregations
            file_name = f'out/{col_name}_{agg}.csv'
            agg_df = ddf_gb.agg({'Unit Sales': agg}).reset_index() # reuse lazy computed gb
            agg_df.to_csv(file_name, index=False, single_file=True) # write out file
            #main_gb.join(agg_df, on=["State", "Item Nbr"]) # join into main file

            #.rename('Unit Sales', 'us_std') \
            #.rename('Unit Sales', 'us_mean')
            #.agg(col_map).reset_index()
            #.sample(frac=0.1)
    #main_gb.to_csv(f'out/combined.csv', index=False, single_file=True) # write out file

    #ddf_us_std.to_csv('out.csv', index=False)

    # ddf .filter(perishable,promotion)
    # LR based on 1,0 on oil vs unit sales per date
    # 2 sample/pop comparison
    # LR based on promotion vs no promotion vs unit sates/transactions
    
    # Pick n item and analyis pre,during,post promotion

@timer
def test_linear_regression(file_name, client):
    ddf = read_file(file_name, use_dask=True, print_cols=True)
    df_pro1 = ddf.loc[ddf['Perishable']==1]
    
    # df_pro0 = ddf.loc[ddf['Perishable']==0]
    # individual transactions probs not valuable - group by item and get mean?
    # exclude outliers for LR
    compute_linear_regression(df_pro1, 'Transactions', 'Unit Sales', plot_graph=True)

@timer
def explore_row_lags(file_name, client):
    df = read_file(file_name, use_dask=True, print_cols=True, nrows=500000)
    client.persist(df)
    dfs = df.sort_values(['Date (Oil.Csv)'], ascending=[True], inplace=True)
    dict_cols = {**dict.fromkeys(dfs.columns, 'last')}
    dict_cols['Date (Oil.Csv)'] = 'diff'
    dfs = dfs.groupby(["Date (Oil.Csv)"]).agg(dict_cols)
    # add unique ids to dataset
    #df['row_number'] = df.assign(partition_count=1).partition_count.cumsum() # very slow bc we load entire dataset into memory
    #df.compute()

    #print(df.columns)
    print(dfs.head())
    dfs.to_csv("out/test.csv", index=False, single_file=True)
    #df_diff.to_csv("out/test.csv", index=False, single_file=True) # write out file

@timer
def explore_psql(file_name, pg_uri, client):
    load_csv_into_db(file_name, pg_uri, client)
    # Note below query does not properly order date due to format MM/dd/yyyy, need yyyy/MM/dd
    """
WITH oil_price_as_date(price, date_oil) AS (
	SELECT 
		MIN(oil_price) as price,
		date_oil
	FROM public.original_data d
	GROUP BY date_oil
	ORDER BY date_oil
),
oil_price_prev(price, date_oil) AS (
	SELECT
		price,
		date_oil,
		LAG(price, 1) OVER (ORDER BY date_oil) prev_price
	FROM oil_price_as_date
	WHERE price IS NOT NULL
)
SELECT price, date_oil, (price-prev_price) as diff FROM oil_price_prev ORDER BY date_oil;
    """

if __name__ == '__main__':
    client = Client() # highly recommend passing client around, just init under main or you will see errors
    #explore_row_lags(FILE_NAME, client)
    #explore_psql(FILE_NAME, PG_URI, client)