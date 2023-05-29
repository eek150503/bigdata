from simdb_utils import simdb_schema, query_write_hive, query_write_file
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession\
    .builder\
    .appName("SimDB Loading")\
    .config("spark.sql.catalogImplementation", "hive")\
    .config("hive.exec.dynamic.partition", "true")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .enableHiveSupport().getOrCreate()


df = spark.read\
    .option("header", "false")\
    .option("delimiter", " ")\
    .option("compression", "gzip")\
    .csv("/user/mis/daily/simdb/2023/05/25/SIMDB183_20230525.csv.gz", schema=simdb_schema)


# replace the columns that have "'" to '' 
df = df.select([F.regexp_replace(c,"'","").alias(c) for c in df.columns])

# create temp table from df
df.createOrReplaceTempView("simdbtmp")

#do some transform and put it hive 
df1 = spark.sql(query_write_hive)
df1.write.option("compression", "snappy").mode('append').format('parquet').partitionBy("yearkey","monthkey","daykey").saveAsTable(('testing.simdb'))

## This take 35mins to completes ##

# take df1, do some transfrom and, writing to file
df1.createOrReplaceTempView("simdbtmp_stg")
df2 = spark.sql(query_write_file)
df2.write.format("com.databricks.spark.csv").options(header='False', delimiter=';').save("/user/mis/combine/simdb_data")


df1.unpersist()
df1.unpersist(True)

df2.unpersist()
df2.unpersist(True)
