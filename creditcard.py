# import pyspark.sql module
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# select the database from mariadb into spark 
# creating the dataframe using spark and the table name is cdw_sapp_creditcard
df2 = spark.read.format("jdbc").options(
    url = "jdbc:mysql://localhost:3306/cdw_sapp",
    driver = "com.mysql.cj.jdbc.Driver",
    dbtable = "cdw_sapp_creditcard",
    user = "root",
    password="").load()

# creating the temp view for PySpark SQL and named table name called credit card
df2.createOrReplaceTempView('creditcard')

# Using spark.sql command to do transformation jobs 
# Note: df2 is the variable for credit card
df2 = spark.sql('SELECT CREDIT_CARD_NO CUST_CC_NO, CONCAT(YEAR, LPAD(Month, 2, 0), LPAD(Day, 2, 0)) TIMEID, CUST_SSN, BRANCH_CODE, TRANSACTION_TYPE, TRANSACTION_VALUE, TRANSACTION_ID FROM creditcard')
df2.show()

# Connecting to MongoDB and the collection name called cdw_sapp_creditcard database name called cdw_sapp
uri = "mongodb://127.0.0.1/cdw_sapp.dbs"
spark_mongodb = SparkSession.builder.config("spark.mongodb.input.uri",uri).config("spark.mongodb.output.uri",uri).getOrCreate()
df2.write.format("com.mongodb.spark.sql.DefaultSource").mode('append').option('database','cdw_sapp').option('collection','cdw_sapp_creditcard').save()
df2.show()
# The result should be 46694 documents
