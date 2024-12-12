import mysql.connector

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Skandmata@5",
        database="youtube_project",
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
