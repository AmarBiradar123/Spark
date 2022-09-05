from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
host="jdbc:mysql://amarsql.cnhk49f8ecfy.ap-south-1.rds.amazonaws.com:3306/mysqldb"

df=spark.read.format("jdbc").option("url",host).option("user","msuser").option("password","mspassword")\
   .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()
df.show()
res=df
#res=df.na.fill(0,["comm"]).withColumn("comm",col("comm").cast(IntegerType()))
#res.write.format("csv").option("header","true").save("s3://amar2022/input")





res=df
res.write.mode("overwrite").format("jdbc").option("url",host).option("user","msuser").option("password","mspassword")\
    .option("dbtable","empclean").option("driver","com.mysql.jdbc.Driver").save()
res.show()
res.printSchema()









