import requests, os
from kafka import KafkaProducer
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

def kafka_prod():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    response = requests.get("https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partOne.txt")
 
    data_list = [data for data in response.text.splitlines()[1:]]
    for data in data_list:
        #print(data)
        producer.send('benefit1', data.encode('utf-8'))
    producer.flush()


def spark_kafka():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    # conf=SparkConf()
    # conf.set("spark.executor.memory", "4g")
    # conf.set("spark.driver.memory", "4g")
    spark = SparkSession.builder.getOrCreate()
    
    raw_kafka_df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("subscribe", 'benefit1') \
                        .option("startingOffsets", "earliest") \
                        .load()

    kafka_value_df = raw_kafka_df.selectExpr("CAST(value AS STRING)")

    output_query = kafka_value_df.writeStream \
                          .queryName("benefit1") \
                          .format("memory") \
                          .start()
    output_query.awaitTermination(10)

    value_df = spark.sql("select * from benefit1")
    value_df.show()
    
    value_rdd = value_df.rdd.map(lambda i: i['value'].split("\t"))
    value_row_rdd = value_rdd.map(lambda i: Row(BenefitName=i[0], \
                                                BusinessYear=i[1], \
                                                EHBVarReason=i[2], \
                                                IsCovered=i[3], \
                                                IssuerId=i[4], \
                                                LimitQty=i[5], \
                                                LimitUnit=i[6], \
                                                MinimumStay=i[7], \
                                                PlanId=i[8], \
                                                SourceName=i[9], \
                                                StateCode=i[10]))

    df = spark.createDataFrame(value_row_rdd)
    df.show()


    
    # df.printSchema()
    
    df.write.format("com.mongodb.spark.sql.DefaultSource") \
         .mode('append') \
         .option('database','health_insurance') \
         .option('collection', 'health_insurance_benefit') \
         .option('uri', "mongodb://127.0.0.1/health_insurance.dbs") \
         .save()

def main():
    kafka_prod()
    spark_kafka()
#     
main()