from pyspark.sql import *
from pyspark.sql.functions import *
import re
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="C:\\BigData\\datasets\\complexxmldata.xml"
df=spark.read.format("xml").option("rowTag","catalog_item").option("path",data).load()
#df.show()
#df.printSchema()
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
#https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar
#https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.2/spark-avro_2.12-3.1.2.jar
op="C:\\BigData\\datasets\\output\\writexml"
ndf.write.mode("overwrite").format("xml").option("rootTag","details").option("rowTag","books").save(op)