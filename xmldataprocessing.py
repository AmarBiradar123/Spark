from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="C:\\BigData\\datasets\\books.xml"
df=spark.read.format("xml").option("rowTag","book").option("path",data).load()
#df.show()
res=df.withColumnRenamed("_id","id").where(col("price")>10)
res.show()
op="C:\\BigData\\datasets\\output\\xml2css.csv"
#res.write.mode("overwrite").format("csv").option("header","true").save(op)
res.toPandas().to_csv(op)