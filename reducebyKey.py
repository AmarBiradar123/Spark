from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\BigData\\datasets\\donations.csv"
drdd=sc.textFile(data)

res=drdd.filter(lambda x: "name" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
    .reduceByKey(lambda x,y:x+y)

for i in res.collect():
    print(i)