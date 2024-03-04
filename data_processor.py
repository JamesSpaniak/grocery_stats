import csv, copy
import pandas as pd
import dask.dataframe as dd

from util import timer

FILE_NAME = "data/train_combined_14_n_16.csv"
    
@timer
def process_data(file_name):
    df = pd.read_csv(file_name)
    df_cols = ['Transactions', 'Unit Sales']
    agg_fns = ['mean', 'std', 'min', 'max', 'count']
    #print(df_cols)
    
    ddf = dd.from_pandas(df, npartitions=8)
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
    #LR -> https://stackoverflow.com/questions/29934083/linear-regression-on-pandas-dataframe-using-sklearn-indexerror-tuple-index-ou

    # ddf .filter(perishable,promotion)
    # LR based on 1,0 on oil vs unit sales per date
    # LR based on promotion vs no promotion vs unit sates/transactions
    
    # Pick n item and analyis pre,during,post promotion


if __name__ == '__main__':
    process_data(FILE_NAME)