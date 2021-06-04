### Snippets

#### Parallel Compute with dask

```{python}
def len_dfs(n):
    prov = providers[(providers['providerId'] >= n) & (providers['providerId'] < (n + prov_range))]
    # off = offices[(offices['providerId'] >= n) & (offices['providerId'] < (n + prov_range))]
    return len(prov)
    
holder = [delayed(len_dfs)(n) for n in range(min_providerId, max_providerId, prov_range)]

holder = dask.compute(*holder)
```

#### Parallel data load with multiprocessing
```{python}
from multiprocessing import Process

def data_load(chunks):

    procs = []
    for chunk in chunks:
        p = Process(target=load_df, args=(chunk, 'providerDirectoryDetails'))
        p.start()
        procs.append(p)

        if (len(procs) == mp.cpu_count()) or (chunk.shape[0] == chunks[-1].shape[0]):
            for proc in procs:
                proc.join()
                procs = []

def load_df(chunk, coll_name):
    db = db_connect()
    collection = db[coll_name]
    if (chunk.shape[0] == 0):
        sys.exit(0)

    collection.insert_many(json.loads(chunk.to_json(orient='records')), ordered=False)
    print("Inserted {} documents in providerDirectoryDetails".format(collection.count_documents({})))
    
 ```
 
 ### pandas dataframe reduce memory
 ```{python}
 def reduce_mem_usage(df, verbose=True):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    start_mem = df.memory_usage().sum() / 1024**2    
    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)    
    end_mem = df.memory_usage().sum() / 1024**2
    if verbose: 
        print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(
            end_mem, 100 * (start_mem - end_mem) / start_mem))
    return df
    ```
