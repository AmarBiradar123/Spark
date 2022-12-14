from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="C:\\BigData\\datasets\\companies.json"
df=spark.read.format("json").option("multiLine","true").load(data)


def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    cols = [re.sub('[^a-zA-Z0-9]', "", c) for c in df.columns]
    return df.toDF(*cols);


ndf=flatten(df)
ndf.printSchema()
ndf.show()
#op="C:\\BigData\\datasets\\output\\result"
#ndf.write.format("csv").option("header","true").save(op)
#git hub reference link
#https://github.com/maroovi/aws_etl/blob/23d5af070f6c605852ff91fdf0f28423548f59e5/glue.py
#referenance..search keyword.."def read_nested_json" pyspark
