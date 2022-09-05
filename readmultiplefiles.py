from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"C:\\BigData\\datasets\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")
spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
#data = "C:\\BigData\\datasets\\10000Records.csv"
df = spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)
import re

cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)
ndf.show(4)
ndf.printSchema()

ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user",user).option("password",pwd)\
.option("dbtable","amarsb").option("driver","com.mysql.jdbc.Driver").save()