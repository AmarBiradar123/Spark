from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="C:\\BigData\\datasets\\world_bank.json"
df=spark.read.format("json").load(data)
ndf=df.withColumn("mjsector_namecode",explode(col("mjsector_namecode")))\
    .withColumn("majorsector_percent",explode(col("majorsector_percent")))\
    .withColumn("mjtheme",explode(col("mjtheme")))\
    .withColumn("mjtheme_namecode",explode(col("mjtheme_namecode")))\
    .withColumn("majorsector_percent_name",col("majorsector_percent.Name"))\
    .withColumn("majorsector_percent_name", col("majorsector_percent.Name"))\
    .withColumn("majorsector_percent_percent", col("majorsector_percent.percent"))\
    .withColumn("mjsector_namecode_code",col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name",col("mjsector_namecode.name"))\
    .withColumn("sector_namecode",explode(col("sector_namecode")))\
    .withColumn("theme_namecode",explode(col("theme_namecode")))\
    .drop("mjsector_namecode","majorsector_percent")\
    .withColumn("id",col("_id.$oid")).drop("_id")

ndf.show(truncate=False)
ndf.printSchema()