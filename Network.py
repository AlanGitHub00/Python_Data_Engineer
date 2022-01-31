import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row

def kafka_prod():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/Network.csv")
 
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
        #print(data)
        producer.send('network1', data.encode('utf-8'))
    producer.flush()


def spark_kafka():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    # conf=SparkConf()
    # conf.set("spark.executor.memory", "4g")
    # conf.set("spark.driver.memory", "4g")
    spark = SparkSession.builder.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/").getOrCreate()
    
    raw_kafka_df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("subscribe", 'network1') \
                        .option("startingOffsets", "earliest") \
                        .load()

    kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")

    output_query = kafka_value_df.writeStream \
                          .queryName("network1") \
                          .format("memory") \
                          .start()
    output_query.awaitTermination(10)

    value_df = spark.sql("select * from network1")
    value_df.show()
    
    value_rdd = value_df.rdd.map(lambda i: i["value"].split(","))
#     value_rdd.foreach(lambda i: print(len(i)))
    value_row_rdd = value_rdd.map(lambda i: Row(BusinessYear=i[0], \
                                                StateCode=i[1], \
                                                IssuerId=int(i[2]), \
                                                SourceName=i[3], \
                                                VersionNum=int(i[4]), \
                                                ImportDate=i[5], \
                                                IssuerId2=int(i[6]), \
                                                StateCode2=i[7], \
                                                NetworkName=i[8], \
                                                NetworkId=i[9], \
                                                NetworkURL=i[10], \
                                                RowNumber=i[11], \
                                                MarketCoverage=i[12], \
                                                DentalOnlyPlan=i[13]))

    df = spark.createDataFrame(value_row_rdd)
    df.show()


    
    # df.printSchema()
    
#     df.write.format("com.mongodb.spark.sql.DefaultSource") \
#         .mode('append') \
#         .option('database','cdw_sapp') \
#         .option('collection', 'cdw_sapp_service') \
#         .option('uri', "mongodb://127.0.0.1/cdw_sapp.dbs") \
#         .save()

    df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode('append') \
        .option('database','health_insurance') \
        .option('collection', 'health_insurance_network') \
        .save()

def main():
    # kafka_prod()
    spark_kafka()
#     
main()
