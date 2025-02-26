import mysql.connector
from botocore.credentials import Credentials
from resources.dev import config
from resources.dev.Credentials import *

def get_mysql_connection():
    connection = mysql.connector.connect(
        host=config.properties['host'],
        user=config.properties['user'],
        password= config.properties['password'],
        database=  config.database_name,
        ssl_disabled=True
    )
    return connection


connection = get_mysql_connection()
cursor = connection.cursor()












# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
