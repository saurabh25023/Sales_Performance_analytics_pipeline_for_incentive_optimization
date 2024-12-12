from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.main.utility.spark_session import spark_session

print("Start")

spark = spark_session()

df1 = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema", "true")\
    .load("C:\\Users\\suraj\\PycharmProjects\\youtube_DE_project\\youtube_DE_project11\\src\\test\\transaction_data5.csv")


# df1.show()
#
# df1.count()
#
# df3 = df1.select("Transaction ID").distinct().count()
# print("df3 = ",df3)
#
# df4 = df1.select("Account Number").distinct().count()
# print("df4=", df4)
#
# df5 = df1.select("Pay Mode").distinct().count()
# print("df5=", df5)
#
# df2 = df1.select("Timestamp").distinct().count()
# print("df2 = ",df2)
#
# df6 = df1.select("Amount").distinct().count()
# print("df6 = ",df6)



print("End")


df10 = (df1.withColumnRenamed("Pay Mode", "pay_mode"))

#df10.show()

# df11 = df10.select("pay_mode").distinct()
#
# df11.show()

df10.printSchema()
df10.show()

df12 = df10.groupby("pay_mode") \
    .agg(round(sum(col("Amount")),2).alias("Total_Amount"))\
    .orderBy(col("Total_Amount"))



df12.show()
