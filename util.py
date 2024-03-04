import dask
dask.config.set({'dataframe.query-planning': True})

import time
import pandas as pd
import dask.dataframe as dd
from functools import wraps

def timer(f):
    @wraps(f)
    def timed(*args, **kw):
        start = time.time()
        result = f(*args, **kw)
        end = time.time()

        print(f'func={f.__name__} args=[{args}, {kw}] took: {end-start:2.4f} sec')
        return result

    return timed

@timer
def read_file(file_name, use_dask=True, print_cols=False):
    df = pd.read_csv(file_name)
    if print_cols:
        print(f'Cols={df.columns}')
    if use_dask:
        return dd.from_pandas(df, npartitions=8)
    return df