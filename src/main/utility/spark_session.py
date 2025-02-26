import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("manish_spark2")\
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark


#C:\\Program Files\\spark\\spark-3.4.4-bin-hadoop3\\jars\\mysql-connector-j-9.2.0.jar


# .config("spark.driver.extraClassPath",
#         "C:\\Program Files\\spark\\spark-3.4.4-bin-hadoop3\\jars\\mysql-connector-j-9.2.0.jar") \

spark_session()