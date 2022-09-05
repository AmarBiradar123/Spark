from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\BigData\\datasets\\bank-full.csv"
ardd=sc.textFile(data)
#convert bank-full to structured data
res=ardd.filter(lambda x: "age" not in x).map(lambda x: re.sub("\"",'',x)).map(lambda x:x.split(";"))
cols = ['age', 'job', 'marital', 'education', 'default', 'balance', 'housing', 'loan', 'contact', 'day', 'month', 'duration', 'campaign', 'pdays', 'previous', 'poutcome', 'y']
#convert this structured data to dataFrame
#toDF is used only 1) convert rdd to dataFrame but data must be structured 2)rename all columns
df=res.toDF(cols)
#df.show() #after creating dataframe it will default give top 20 rows
#res1=df.where((df.balance>50000) & (df.marital=='single'))
res1=df.where((col("balance")>50000) & (col("marital")=='single'))
df.createOrReplaceTempView("Tab")
#res1=spark.sql("select * from tab where balance>40000 and marital='single'")
res1.show()
#res1.show()


#for i in res.take(9):
#    print(i)




