import os
import pandas as pd
import shutil

def re_partition(load_dt):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/data/movie/movie_data/data/extract/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie/repartition'  # 상위 디렉토리 - 데이터 저장의 기본 위치
    write_path = f'{write_base}/load_dt={load_dt}'  # 하위 디렉토리 - 날짜별,측정 기준으로 파티션된 최종적으로 저장할 구체적인 디렉토리

    df = pd.read_parquet(read_path)
    df['load_dt']=load_dt
    rm_dir(write_path)
    df.to_parquet(write_path, partition_cols=['load_dt','multiMovieYn', 'repNationCd'])
    #return len(df), read_path, wirte_path
    
def rm_dir(write_path):
    if os.path.exists(write_path):
        shutil.rmtree(write_path)
