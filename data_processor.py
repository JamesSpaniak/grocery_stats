import dask
dask.config.set({'dataframe.query-planning': True})

import csv
import pandas as pd
import dask.dataframe as dd

from util import timer, read_file, compute_linear_regression

FILE_NAME = "data/train_combined_14_n_16.csv"

@timer
def process_data(file_name):
    ddf = read_file(file_name, use_dask=True, print_cols=True)
    df_cols = ['Transactions', 'Unit Sales']
    agg_fns = ['mean', 'std', 'min', 'max', 'count']
    
    ddf_gb = ddf.groupby(["State", "Item Nbr"]) # Lazy compute
    #ddf_gb.persist() # cache

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
    # LR based on promotion vs no promotion vs unit sates/transactions
    
    # Pick n item and analyis pre,during,post promotion

def run_linear_regression(file_name):
    ddf = read_file(file_name, use_dask=True, print_cols=True)
    df_pro1 = ddf.loc[ddf['Perishable']==1]
    
    # df_pro0 = ddf.loc[ddf['Perishable']==0]
    # individual transactions probs not valuable - group by item and get mean?
    # exclude outliers for LR
    compute_linear_regression(df_pro1, 'Transactions', 'Unit Sales',plot_graph=True)

if __name__ == '__main__':
    run_linear_regression(FILE_NAME)