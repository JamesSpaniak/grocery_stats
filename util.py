
import dask
dask.config.set({'dataframe.query-planning': True})

import time
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
from functools import wraps
from sklearn import linear_model

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
def read_file(file_name, use_dask=True, print_cols=False, nrows=-1):
    df = None
    if nrows<0:
        df = pd.read_csv(file_name)
    else:
        df = pd.read_csv(file_name, nrows=nrows)
    if print_cols:
        print(f'Cols={df.columns}')
    if use_dask:
        return dd.from_pandas(df, npartitions=8)
    return df

@timer
def compute_linear_regression(df, xcol, ycol, plot_graph=False): # [(col_name, value)]
    x_data = df[xcol].values
    y_data = df[ycol].values

    x_data.compute_chunk_sizes()
    y_data.compute_chunk_sizes()

    length = len(x_data)
    x_data = x_data.reshape(length, 1)
    y_data = y_data.reshape(length, 1)

    regr = linear_model.LinearRegression()
    regr.fit(x_data, y_data)

    if plot_graph:
        plt.scatter(x_data, y_data,  color='black')
        plt.plot(x_data, regr.predict(x_data), color='blue', linewidth=3)
        plt.xticks(())
        plt.yticks(())
        plt.show()