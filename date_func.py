from pyspark.sql import *
from pyspark.sql.functions import *

from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone","EST").getOrCreate()
data="C:\\BigData\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

#create udf to get expected date format. Like 1Yr, 2Months, 4Days
def daystoyrmndays(nums):
    yrs = int(nums / 365)
    mon = int((nums % 365) / 30)
    days = int((nums % 365) % 30)
    result = yrs, "years" , mon , "months" , days, "days"
    st = ''.join(map(str, result))
    return st

udffunc = udf(daystoyrmndays)
#.withColumn("daystoyrmon", udffunc(col("dtdiff")))

#spark by default able to understand yyyy-MM-dd format but u have in dd-MM-yyyy
res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy")).withColumn("today",current_date())\
    .withColumn("ts",current_timestamp()).withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),100)).withColumn("dtsub",date_sub(col("dt"),100))\
    .withColumn("lastdt",date_format(last_day(col("dt")),"yyyy-MM-dd-EEE")).withColumn("nxtdt",next_day(col("dt"),"Sun"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/zzz"))\
    .withColumn("MonthLstFri",next_day(date_add(last_day(col("dt")),-7),"Fri"))\
    .withColumn("dayofWeek", dayofweek(col("dt")))\
    .withColumn("dayodmon",dayofmonth(col("dt")))\
    .withColumn("dayofyr",dayofyear(col("dt")))\
    .withColumn("monbet",months_between(current_date(),col("dt")))\
    .withColumn("Yr",year(col("dt")))\
    .withColumn("mnt",month(col("dt")))\
    .withColumn("floor",floor(col("monbet")))\
    .withColumn("ceil",ceil(col("monbet")))\
    .withColumn("round",round(col("monbet")).cast(IntegerType()))\
    .withColumn("dttrunc",date_trunc("month",col("dt")))\
    .withColumn("daystoyrmon", udffunc(col("dtdiff")))
res.printSchema()
res.show(truncate=False)
