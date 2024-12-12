from pyspark.sql.types import *

from src.main.utility.spark_session import *

spark = spark_session()

# schema = StructType([
#     StructField("customer_id", IntegerType(), True),
#     StructField("store_id", IntegerType(), True),
#     StructField("product_name", StringType(), True),
#     StructField("sales_date", DateType(), True),
#     StructField("sales_person_id", IntegerType(), True),
#     StructField("price", FloatType(), True),
#     StructField("quantity", IntegerType(), True),
#     StructField("total_cost", FloatType(),True),
#     StructField("additional_column", StringType(),True)
# ])

# data = [1,121,'chocolate','2024-05-01',4,54.3,5,122,'abc']
#
# df = spark.createDataFrame(data=data,schema=schema)
# df.show()

data2 = [1,'ramesh',23]
schema2 = StructType([
    StructField("id", IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True)
                    ])
df3=spark.createDataFrame([(1,"ram",4),(2,"shyam",5)],schema2)
#df2 = spark.createDataFrame(data=data2,schema=schema2)
df3.show()

