"""SimpleApp.py"""
from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
LOAD_DT = sys.argv[2]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

#logFile = "/home/joo/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system
#logData = spark.read.text(logFile).cache()

#numAs = logData.filter(logData.value.contains('a')).count()
#numBs = logData.filter(logData.value.contains('b')).count()

df1 = spark.read.parquet(f"/home/joo/data/movie/repartition/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("df1")

result_df = spark.sql(f"""
    SELECT 
        movieCd, -- 영화의 대표코드
        movieNm,
        salesAmt, -- 매출액
        audiCnt, -- 관객수
        showCnt, --- 사영횟수
        multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
        repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
        {LOAD_DT} AS load_dt
    FROM df1
""")

result_df.show()

spark.stop()
