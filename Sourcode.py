# trigger kafka batch job from the kakfka topics.
# Install this libraries in databricks.
# run this code in databricks.


# get these details from confluent kafka, search it on google.
confluentBootstrapServers = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
confluentApiKey = 'XRH2SVIJU7FYNBWV'
confluentSecret = 'kl7k55Ri9mLD+UhI9crfnw146w+WamTumbaLoHP2+YnJJP+s3Dl5u8b2ED+8ILBX'
# we created the topic in confluent kafka.
confluentTopicName = 'topic_kafka_vs'

if __name__ == '__main__':
  
   orders_df = spark \
   .readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers",confluentBootstrapServers) \
   .option("kafka.security.protocol","SASL_SSL") \
   .option("kafka.sasl.mechanism","PLAIN") \
   .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
   .option("kafka.ssl.endpoint.identification.algorithm","https") \
   .option("subscribe",confluentTopicName) \
   .option("startingTimestamp", 1) \
   .option("maxOffsetsPerTrigger", 50) \
   .load()
   # it read the topic from the very starting - startingTimestamp
   # microbatches are of same size - maxOffsetsPerTrigger


   print(orders_df.head(10))


   converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")


   print(converted_orders_df.head(10))


   converted_orders_df \
   .writeStream \
   .queryName("ingestionquery") \
   .format("delta") \
   .outputMode("append") \
   .option("checkpointLocation","checkpointdir301") \
   .toTable("orderstablenew301")
   # .toTable is an actions, rest were transaformation while writing the df.
   # delta table will persist at some locations (in harddrisk), kakfka topic data are available only gfor 7 days by default.
   # to create this table (orderstablenew301), it needs as hive warehouse directory.
   # databricks have already spark session available to them. 

   spark.sql("select * from orderstablenew301").show()






