"""movie_sum_df.py"""
from pyspark.sql import SparkSession
import sys
import shutil

LOAD_DT = sys.argv[1] # {{ds_nodash}}

spark = SparkSession.builder.appName("agg_df").getOrCreate()

df = spark.read.parquet(f"/home/joo/data/movie/hive/load_dt={LOAD_DT}")
df.createOrReplaceTempView("one_hive_df")
df.show()

df_m = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    multiMovieYn,
    '{LOAD_DT}' AS load_dt

FROM one_hive_df
GROUP BY multiMovieYn
""")
df_m.show()

df_m.write.mode('overwrite').partitionBy("load_dt").parquet(f"/home/joo/data/movie/sum_multi")

df_n = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    repNationCd,
    '{LOAD_DT}' AS load_dt

FROM one_hive_df
GROUP BY repNationCd
""")
df_n.show()

df_n.write.mode('overwrite').partitionBy("load_dt").parquet(f"/home/joo/data/movie/sum_nation")


spark.stop()
