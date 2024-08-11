"""movie_join_df.py"""
from pyspark.sql import SparkSession
import sys
import os
import shutil

#APP_NAME = sys.argv[1] # 대그 배시 커맨드라인 인자 받아오기,JOIN_TASK_APP
LOAD_DT = sys.argv[1] # {{ds_nodash}}

spark = SparkSession.builder.appName("agg_df").getOrCreate()

df = spark.read.parquet(f"/home/joo/data/movie/repartition/load_dt={LOAD_DT}")
df.createOrReplaceTempView("df")
df.show()

df1 = spark.sql(f"""
SELECT
    movieCd,
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt,
    multiMovieYn,
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
    
FROM df
WHERE multiMovieYn IS NULL
""")

df1.createOrReplaceTempView("multi_null")
df1.show()

df2 = spark.sql(f"""
SELECT
    movieCd,
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt,
    multiMovieYn,
    repNationCd,-- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
    
FROM df
WHERE repNationCd IS NULL
""")

df2.createOrReplaceTempView("nation_null")
df2.show()

df_join = spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.movieNm, n.movieNm) AS movieNm,
    COALESCE(m.salesAmt, n.salesAmt) AS salesAmt, -- 매출액
    COALESCE(m.audiCnt, n.audiCnt) AS audiCnt, -- 관객수
    COALESCE(m.showCnt, n.showCnt) AS showCnt,
    COALESCE(m.multiMovieYn, n.multiMovieYn) AS multiMovieYn,
    COALESCE(m.repNationCd, n.repNationCd) AS repNationCd,-- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
    
FROM multi_null m FULL OUTER JOIN nation_null n 
ON m.movieCd=n.movieCd
""")

df_join.createOrReplaceTempView("df_join")
df_join.show()

SAVE_BASE = "/home/joo/data/movie/hive"
SAVE_PATH = f"{SAVE_BASE}/load_dt={LOAD_DT}"

if os.path.exists(SAVE_PATH):
        shutil.rmtree(SAVE_PATH)

df_join.write.mode('overwrite').partitionBy("multiMovieYn", "repNationCd").parquet(f"/home/joo/data/movie/hive/load_dt={LOAD_DT}")
#df_join.write.mode('append').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/joo/data/movie/hive")

df_join.show()


spark.stop()
