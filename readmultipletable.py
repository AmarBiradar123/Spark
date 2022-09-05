from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"C:\\BigData\\datasets\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")
qry="(select table_name from information_schema.tables where TABLE_SCHEMA='mysqldb') aaa";

df1=spark.read.format("jdbc").option("url", host).option("user",user).option("password",pwd) \
        .option("dbtable",qry).option("driver", "com.mysql.jdbc.Driver").load()
tabs=[x[0] for x in df1.collect() if df1.count()>0]

for i in tabs:
        df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
        .option("dbtable",i).option("driver","com.mysql.jdbc.Driver").load()
        df.show()
