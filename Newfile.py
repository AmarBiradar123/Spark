from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\BigData\\Dataset\\drivers\\asl.csv"
ardd = sc.textFile(data)
#skip=ardd.first()
res=ardd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).toDF(["name","age","city"])
res.createOrReplaceTempView("tab")
#result=spark.sql("select * from tab where city='blr' and age<30")
result=res.where((col("age")<=30) & (col("city")=="blr"))
result.show()
for i in res.collect():
    print(i)
