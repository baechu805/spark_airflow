import os
import pandas as pd

def re_partition(load_dt):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/data/movie/movie_data/data/extract/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie/repartition'
    write_path = f'{write_base}/load_dt={load_dt}'

    df = pd.read_parquet(read_path)
    df['load_dt']=load_dt
    df.to_parquet(write_base, partition_cols=['load_dt','multiMovieYn', 'repNationCd'])
