from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="C:\\BigData\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").option("sep",",").load(data)

import re
cols=[re.sub('[^a-zA-Z0-9]',"",c) for c in df.columns]
ndf=df.toDF(*cols)

ndf.show(21,truncate=False)
df.printSchema()
#jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb
host="jdbc:mysql://amarsql.cnhk49f8ecfy.ap-south-1.rds.amazonaws.com:3306/mysqldb"
uname="msuser"
pwd="mspassword"

ndf.write.mode("overwrite").format("jdbc").option("url",host)\
    .option("dbtable","amar3").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").save()
