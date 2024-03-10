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
    #load_csv_into_db(file_name, pg_uri, client)
    # Note below query does not properly order date due to format MM/dd/yyyy, need yyyy/MM/dd
    data_query = """
        WITH oil_price_as_date(price, f_date, transactions_count, at_size) AS (
            SELECT 
                MIN(oil_price) AS price,
                TO_DATE(date_oil, 'MM/DD/YYYY') AS f_date,
                SUM(transactions) AS transactions_count,
                SUM(unit_sales) AS at_size
            FROM public.original_data d
            GROUP BY f_date
        ),
        oil_price_prev(price, f_date, prev_price, transactions_count, tc_prev, size_per_t) AS (
            SELECT
                price,
                f_date,
                LAG(price, 1) OVER (ORDER BY f_date) prev_price,
                transactions_count,
                LAG(transactions_count, 1) OVER (ORDER BY f_date) tc_prev,
                transactions_count/at_size AS size_per_t
            FROM oil_price_as_date
            WHERE price IS NOT NULL
        )
        SELECT
            price,
            (transactions_count/1000) as tc_k,
            f_date,
            size_per_t,
            (o.price-o.prev_price) as diff,
            ((o.price-o.prev_price)/o.prev_price) as pct_change,
            (ln(o.price) - ln(o.prev_price)) as log_return,
            ((o.transactions_count-o.tc_prev)/o.tc_prev) as tc_pct_change
        FROM oil_price_prev o
        ORDER BY f_date
    """
    engine = create_engine(pg_uri)
    conn = engine.connect()
    #res = conn.execute(data_query)
    df = pd.read_sql(data_query, conn)
    #df.columns = res.keys()
    print(df.head())
    compute_linear_regression(df, "price", "size_per_t", True)

if __name__ == '__main__':
    client = Client() # highly recommend passing client around, just init under main or you will see errors
    #explore_row_lags(FILE_NAME, client)
    #explore_psql(FILE_NAME, PG_URI, client)
    explore_psql(FILE_NAME, PG_URI, client)