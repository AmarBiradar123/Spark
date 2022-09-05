from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\BigData\\datasets\\donationsal.csv"
artt=sc.textFile(data)

res=artt.filter(lambda x: "dt" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
    .reduceByKey(lambda x,y:x+y)
op="C:\\BigData\\datasets\\output\\groupdon"
res.saveAsTextFile(op)
for i in res.take(2):
    print(i)
