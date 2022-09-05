from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\BigData\\datasets\\emailsmay4.txt"
drdd=sc.textFile(data)

res=drdd.flatMap(lambda x:x.split(" ")).filter(lambda x: "@" in x)

for i in res.collect():
    print(i)