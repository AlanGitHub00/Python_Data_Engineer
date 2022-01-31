# import pyspark.sql module
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# select the database from mariadb into spark 
# creating the dataframe using spark and the table name is cdw_sapp_customer
df3 = spark.read.format("jdbc").options(
    url = "jdbc:mysql://localhost:3306/cdw_sapp",
    driver = "com.mysql.cj.jdbc.Driver",
    dbtable = "cdw_sapp_customer",
    user = "root",
    password="").load()

# creating the temp view for PySpark SQL and named table name called credit card
df3.createOrReplaceTempView('customer')

# Using spark.sql command to do transformation jobs 
# Note: df3 is the variable for customer
df3 = spark.sql("SELECT SSN CUST_SSN, \
       CONCAT(UCASE(SUBSTRING(`FIRST_NAME`, 1, 1)), LOWER(SUBSTRING(`FIRST_NAME`, 2))) AS CUST_F_NAME, \
       CONCAT(LCASE(SUBSTRING(`MIDDLE_NAME`, 1, 1)), LOWER(SUBSTRING(`MIDDLE_NAME`, 2))) AS CUST_M_NAME, \
       CONCAT(UCASE(SUBSTRING(`LAST_NAME`, 1, 1)), LOWER(SUBSTRING(`LAST_NAME`, 2))) AS CUST_L_NAME, \
       CREDIT_CARD_NO CUST_CC_NO, CONCAT(APT_NO, ' ', STREET_NAME) AS CUST_STREET, \
       CUST_CITY, \
       CUST_STATE, \
       CUST_COUNTRY, \
       CUST_ZIP, \
       CONCAT(SUBSTRING(cust_phone,1,2), '-', SUBSTRING(cust_phone,3,2), '-', SUBSTRING(cust_phone,5,7)) AS CUST_PHONE, \
       CUST_EMAIL, \
       LAST_UPDATED \
       FROM customer")
df3.show()

# Connecting to MongoDB and the collection name called cdw_sapp_customer database name called cdw_sapp
uri = "mongodb://127.0.0.1/cdw_sapp.dbs"
spark_mongodb = SparkSession.builder.config("spark.mongodb.input.uri",uri).config("spark.mongodb.output.uri",uri).getOrCreate()
df3.write.format("com.mongodb.spark.sql.DefaultSource").mode('append').option('database','cdw_sapp').option('collection','cdw_sapp_customer').save()
df3.show()
# The result should be 952 documents
