from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data = "C:\\BigData\\datasets\\donations1.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)
df.printSchema()
df.show()


