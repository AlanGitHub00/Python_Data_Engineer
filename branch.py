# Import mysql.connect module 
# Using Python to extract data from tables stored in MariaDB
import mysql.connector as mariadb
mariadb = mariadb.connect(user = "root", password = "", database = "cdw_sapp")
print('Connected to MariaDB database ...')
cur = mariadb.cursor()

# import pyspark.sql module
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# select the database from mariadb into spark 
# creating the dataframe using spark and the table name is cdw_sapp_creditcard
df1 = spark.read.format("jdbc").options(
    url = "jdbc:mysql://localhost:3306/cdw_sapp",
    driver = "com.mysql.cj.jdbc.Driver",
    dbtable = "cdw_sapp_branch",
    user = "root",
    password="").load()

# creating the temp view for PySpark SQL and named table name called credit card
df1.createOrReplaceTempView('branch')

# Using spark.sql command to do transformation jobs 
# Note: df2 is the variable for credit card

# Using spark.sql command to do transformation jobs 
# Note: df1 is the variable for branch
df1 = spark.sql("SELECT BRANCH_CODE, BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, BRANCH_STATE, IFNULL(branch_zip, 99999) BRANCH_ZIP, CONCAT('(',SUBSTR(branch_phone,1,3),') ',SUBSTR(branch_phone,4,3),'-', SUBSTR(branch_phone,7)) AS BRANCH_PHONE, LAST_UPDATED FROM branch")
df1.show()

# Connecting to MongoDB and the collection name called cdw_sapp_branch database name called cdw_sapp
uri = "mongodb://127.0.0.1/cdw_sapp.dbs"
spark_mongodb = SparkSession.builder.config("spark.mongodb.input.uri",uri).config("spark.mongodb.output.uri",uri).getOrCreate()
df1.write.format("com.mongodb.spark.sql.DefaultSource").mode('append').option('database','cdw_sapp').option('collection','cdw_sapp_branch').save()
df1.show()
# The result should be 115 documents