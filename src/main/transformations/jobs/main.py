import datetime
import os.path
import shutil

from pyspark.sql.functions import concat_ws, lit
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, DateType, FloatType

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.sales_mart_calculation_table_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import *
from src.main.utility.my_sql_session import *
from src.main.utility.spark_session import spark_session
from resources.dev.config import *
from src.main.transformations.jobs.dimension_tables_join import *
from src.main.write.parquet_writer import ParquetWriter

#1********************************************************************************************************************
#1********************************* Job1 = Prerequisite *****************************************************************************
#1********************************************************************************************************************


############# Get S3 Client ###################
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()


##### Now you can use s3_client for your S3 operations

response = s3_client.list_buckets()
print(response)
logger.info("List of Buckets: %s", response['Buckets'])

# (1). check if local directory has already a file
#   (a). If there is a file then check if the same file is present in the staging area with status as 'A'
#           If so, then don't delete and try to re-run
#   (b). Else, give an error and do not process the next file.

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
print("check1001")
print("csv_files=",csv_files)

connection = get_mysql_connection()
cursor = connection.cursor()

print("----check MySQL----")
print("connection=",connection)
print("cursor=",cursor)


total_csv_files = []
print("check1002")
print(total_csv_files)
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    print("total_csv_files=",total_csv_files)
    statement = f"""select distinct file_name from {config.database_name}.{config.product_staging_table}
                    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A' ;
                """

    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()
    print('data--->',data)

    if data:
        logger.info("Your last run was failed please check")

    else:
        logger.info("No record match!")

else:
    logger.info("last run was successful!!!")

#1*********************J1check1 is completed****************************************************************

# (2).checking files at s3 bucket
#       (a). if available then, print their absolute path
#       (b). if not available then raise exception -- no data available to process.
#
try:
    s3_reader = S3Reader() #3330
    # Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)

    logger.info("Absolute path on s3 bucket for csv file %s ", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No Data available to process ")

except Exception as e:
    logger.error("Exited with error:- %s", e)
    raise e

#1********************************* J1check2 is completed **************************************************

#['s3://myoutube-project1/sales_data/file1.csv', 's3://myoutube-project1/sales_data/file2.csv', 's3://myoutube-project1/sales_data/file3.txt']
##########################################

#2******************************************************************************************************************
#2******************************************** Job2 = Download to local ***************************************************************
#2*******************************************************************************************************************

# J2Check1 --- Downloading files from s3 to local_directory

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"

file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
#print("file_path--->",file_paths)
logging.info("File path available on s3 under %s bucket and folder name is %s", bucket_name,file_paths)
logging.info(f"File path available on s3 under {bucket_name} bucket and folder name is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client,bucket_name, local_directory)
    downloader.download_files(file_paths)

except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

# J2Check2 -- Check downloaded files --
#
#Get a list of all files in the local directory

all_files = os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download {all_files}")

# J2Check3 -- check *.csv files at local
#           Filter files with ".csv" in their names and create absolute paths

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("There is nothing data to process at local.")
    raise Exception("There is nothing data to process at local.")

#J2Check4 -- list a final total no. of csv files that are going to process

logger.info("*************************Listing the File*********************")
logger.info("List of csv files that needs to be processed %s", csv_files)

#********************************************************************************************************************
#**************************** Job3 = spark session ******************************************************************
#********************************************************************************************************************

logger.info("***************Creating spark session*********************")

spark = spark_session()

logger.info("************************* spark session created ***********************")

#check the required column in the schema of csv files
#If required columns not found/missing then, keep it in a list or "error_files"
#else (means files are appropriate) union all the data into one dataframe

logger.info("*********** checking Schema for data loaded in S3 ******************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header","true")\
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)

    else:
        logger.info((f"No missing column for the {data}"))
        correct_files.append(data)

    logger.info(f"******************* List of correct files **********************{correct_files}")
    logger.info(f"******************* List of error files ***********************{error_files}")
    logger.info("******************* Moving Error data file  to error directory if any ******************")

#Move the data to error directory on local
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)
            print("file_path=", file_path)
            print("file_name--->",file_name)
            print("destination-->", destination_path)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' from s3 file path to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"Operation on S3--->{message}")
        else:
            logger.error(f"'{file_path}' does not exist.")
    else:
        logger.info("************* There is no error files available at our dataset ********************")

#********************************************************************************************************************
#*************************************** Job 4 = additonal column ***************************************************
#********************************************************************************************************************

#Additional columns needs to be taken care of
# Determine extra columns

#Before running the process
#stage table needs to be updated with status as Active(A) or inactive(I)

logger.info(f"******** Updating the product_staging_table that we have started the process **********")
insert_statements =[]
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:

        filename = os.path.basename(file)
        file = fr'{file}'
        statements = rf"""
                INSERT INTO {db_name}.{config.product_staging_table}
                (file_name, file_location, created_date, status)
                VALUES('{filename}','{file}','{formatted_date}','A')
                 """
        print('#################  Check 223  ###########################################################')
        print('file_name-->', filename)
        print('file_location-->', file)

        insert_statements.append(statements)


    logger.info(f"Insert statement created for staging table ----{insert_statements}")
    logger.info("*********************Connecting to MySQL server*************************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("******************* MySQL server connected Successfully!!!*********************")

    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("************* There is no files to process.....************************")
    raise Exception("************** No Data available with correct files ********************")

logger.info("****************** Fixing extra column coming from source **************************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(),True),
    StructField("additional_column", StringType(),True)
])

#Connecting with DatabaseReader
database_client = DatabaseReader(config.url,config.properties)
logger.info("********************** creating empty dataframe *************************")
# final_df_to_process = spark.createDataFrame([],schema)
# final_df_to_process.show()

final_df_to_process = database_client.create_dataframe(spark,"empty_df_create_table")
final_df_to_process.show()

#Create a new column with concatenated values of extra columns
for data in correct_files:
    data_df = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_columns", concat_ws(", ", *extra_columns))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                    "price","quantity","total_cost","additional_columns")
        logger.info(f"processed {data} and added 'additional columns'")
    else:
        data_df = data_df.withColumn("additional_columns", lit(None))\
                .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                    "price","quantity","total_cost","additional_columns")
    final_df_to_process = final_df_to_process.union(data_df)

logger.info("********************Final DataFrame from source which will be going to processing")
print("final_df_to_process--->")
final_df_to_process.show()

#Enrich the data from all dimension table
#also create a datamart for sales_team and their incentive, address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that
#there should be a store_id segregation
#Read the data from parquet and generate a csv file
#in which there will be a sales_person_name,sales_person_store_id
#sales_person_total_billing_done_for_each_month, total_incentive

#connecting with DatabaseReader
database_client = DatabaseReader(config.url,config.properties)
#creating df for all tables
#customer table
logger.info("************** Loading customer table into customer_table_df ******************")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)

logger.info("*************** Loading product table into product_table_df **********************")
product_table_df = database_client.create_dataframe(spark,config.product_table)

#product_staging_table
logger.info("**************** Loading staging table into product_staging_table *********************")
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)

#sales_team table
logger.info("****************** Loading sales team table into sales_team_table_df ***********************")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

#store_table
logger.info("****************** Loading store table into store_table_df *********************************")
store_table_df = database_client.create_dataframe(spark,store_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                       customer_table_df,
                                                       store_table_df,
                                                       sales_team_table_df)
#Final enriched data
logger.info("***************** Final Enriched Data *********************")
s3_customer_store_sales_df_join.show()



#Write the customer data into customer data mart in parquet format
#file will be written to local first
#move the raw data to s3 bucket for reporting tool
#Write reporting data into MySql table also
logger.info("***************** write the data into customer data mart **********************")
final_customer_data_mart_df = s3_customer_store_sales_df_join \
    .select("ct.customer_id",
            "ct.first_name","ct.last_name","ct.address",
            "ct.pincode","phone_number",
            "sales_date","total_cost")
logger.info("****************** Final Data for customer Data Mart **************************")
final_customer_data_mart_df.printSchema()
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"******************** customer data written to local disk at {config.customer_data_mart_local_file}")

#Move data on s3 bucket for customer_data_mart
logger.info("******************** Data movement from local to s3 for customer data mart *****************************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#sales_team Data Mart
logger.info("*********************** Write the data into sales team Data Mart **********************************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
            .select("store_id",
                    "sales_person_id","sales_person_first_name","sales_person_last_name",
                    "store_manager_name","manager_id","is_manager",
                    "sales_person_address","sales_person_pincode",
                    "sales_date","total_cost", expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("************************* Final Data for sales team Data Mart ***********************************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)
logger.info(f"**************** Sales team data written to local disk at {config.sales_team_data_mart_local_file}")

#Move data to s3 bucket for sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#Also writing the data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
        .option("header","true")\
        .mode("overwrite")\
        .partitionBy("sales_month","store_id")\
        .option("path",config.sales_team_data_mart_partitioned_local_file)\
        .save()

#Move data on s3 for partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)

#Calculation for customer mart

#find out the customer total purchase every month
#write the data into MySql table
logger.info("********** Calculating customer every month purchased amount ***************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("************ Calculation of customer mart done and written into the table*************")

# calculation for sales team mart
# find out the total sales done by each person every month
# Give the top performer 1% incentive of total sales of the month
# Rest sales person will get nothing
# Write the data into Mysql table

logger.info("********** Calculating sales every month billed amount ***************")

sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("************ Calculation of sales mart done and written into the table *************")

######################## Last STEP #######################################
# Move the file on S3 into processed folder and delete the local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"{message}")

logger.info("*************** Deleting files_from_S3 at local ******************")
delete_local_file(config.local_directory)
logger.info("************  Deleted files_from_S3 at local ***************")

logger.info("************** Deleting customer_data_mart_files from local ****************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("*************** Deleted customer_data_mart_files from local *******************")

logger.info("*************** Deleting sales_team_data_mart files from local **********************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("*************** Deleted sales_team_data_mart files form local ***********************")

logger.info("************** Deleting sales_team_data_mart partitioned local files from local ****************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("************** Deleted sales_team_data_mart partitioned local files from local ****************")

#update the status of staging table
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""
            UPDATE {db_name}.{config.product_staging_table} SET status = 'I',updated_date = '{formatted_date}'  WHERE file_name = '{filename}';
        """
        update_statements.append(statements)

    logger.info(f"Updated statement created for staging table ----- {update_statements}")
    logger.info("***************** Connecting with MySQL server **************************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("**************** MySQL server connected SUCCESSFULLY!!! *************************")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("**************** There are some error in process in between ***********************")
    sys.exit()

input("press enter to terminate")





