landing_zone = '/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/'
orders_data = landing_zone + 'orders_data'
checkpoint_path = landing_zone + 'orders_checkpoint'

%sql
show catalogs;

-- catalog
-- misgaurav_catalog
-- misgaurav_catalog_new
-- misgaurav_databricks_ws_7405615643942288
-- samples
-- system

%sql
GRANT USE CATALOG ON CATALOG misgaurav_databricks_ws_7405615643942288 TO `gauravmishra7080@gmail.com`;
GRANT USE SCHEMA ON SCHEMA misgaurav_databricks_ws_7405615643942288.default TO `gauravmishra7080@gmail.com`;
GRANT CREATE TABLE ON SCHEMA misgaurav_databricks_ws_7405615643942288.default TO `gauravmishra7080@gmail.com`;

%sql
use catalog misgaurav_databricks_ws_7405615643942288;
use schema default;
    
show tables;
-- database	tableName	isTemporary
-- 	        _sqldf	   true


#============================
# Reading from a kakfka topic =
#============================

# first create a pipeline to read the data from the streaming sources, then create a pipeline to write the data somewhere else.

# get these details from confluent kafka, search it on google.
confluentBootstrapServers = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
confluentApiKey = '2JYEF54HVRAGH62C'
confluentSecret = 'cfltlPlBW6OzUBDjF6opu4WA08WOPzixKNfNFY+55pgKwCreRSNz50om4sAbmoGQ'
# we created the topic in confluent kafka.
confluentTopicName = 'topic_0'


  
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


converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")

query = converted_orders_df \
   .writeStream \
   .queryName("ingestionquery") \
   .format("delta") \
   .outputMode("append") \
   .option("checkpointLocation",checkpoint_path) \
   .toTable("misgauravorderstablenew") \

print(query.status)
   # .toTable is an actions, rest were transaformation while writing the df.
   # delta table will persist at some locations (in harddrisk), kakfka topic data are available only gfor 7 days by default.
   # to create this table (orderstablenew301), it needs as hive warehouse directory.
   # databricks have already spark session available to them. 
# queryName("ingestionquery").start()


# trigger kafka batch job.

# insert the data: orders_input.json

# create API key and secret in confluent cloud once after creating a cluster.
# then attatch the API key and secret to the topic you create in confluent cloud.
# also check the bootstrap servers.

# Install this libraries in databricks.
# here: https://adb-7405611670894568.8.azuredatabricks.net/compute/clusters/1231-074055-14oqu1ej/libraries?o=7405611670894568
# confluent-kafka[avro,json,protobuf]>=1.4.2

# run this code in databricks.


from confluent_kafka import Producer

import socket, json

# https://confluent.cloud/environments/env-v8jrqz/clusters/lkc-mo88oq/settings/kafka
conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '2JYEF54HVRAGH62C',
        'sasl.password': 'cfltlPlBW6OzUBDjF6opu4WA08WOPzixKNfNFY+55pgKwCreRSNz50om4sAbmoGQ',
        'client.id': 'ccloud-python-client-c0aeab10-5a58-4ce7-9bbb-e9f175ef853d'}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_data/orders_input.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = 'topic_0', key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()


spark.sql("select * from misgauravorderstablenew").show()

# +-----+--------------------+-------+---------+------+--------------------+-------------+
# |  key|               value|  topic|partition|offset|           timestamp|timestampType|
# +-----+--------------------+-------+---------+------+--------------------+-------------+
# |  256|{"order_id":2,"cu...|topic_0|        1|     0|2026-01-03 07:13:...|            0|
# | 4530|{"order_id":7,"cu...|topic_0|        1|     1|2026-01-03 07:13:...|            0|
# |  918|{"order_id":11,"c...|topic_0|        1|     2|2026-01-03 07:14:...|            0|
# | 9149|{"order_id":13,"c...|topic_0|        1|     3|2026-01-03 07:14:...|            0|
# | 7276|{"order_id":16,"c...|topic_0|        1|     4|2026-01-03 07:14:...|            0|
# | 5657|{"order_id":9,"cu...|topic_0|        2|     0|2026-01-03 07:14:...|            0|
# | 5648|{"order_id":10,"c...|topic_0|        2|     1|2026-01-03 07:14:...|            0|
# | 9842|{"order_id":14,"c...|topic_0|        2|     2|2026-01-03 07:14:...|            0|
# | 8827|{"order_id":4,"cu...|topic_0|        4|     0|2026-01-03 07:13:...|            0|
# |11318|{"order_id":5,"cu...|topic_0|        4|     1|2026-01-03 07:13:...|            0|
# | 9488|{"order_id":19,"c...|topic_0|        4|     2|2026-01-03 07:14:...|            0|
# | 9198|{"order_id":20,"c...|topic_0|        4|     3|2026-01-03 07:14:...|            0|
# |  656|{"order_id":28,"c...|topic_0|        3|     1|2026-01-03 07:14:...|            0|
# | 6983|{"order_id":31,"c...|topic_0|        3|     2|2026-01-03 07:14:...|            0|
# | 5225|{"order_id":50,"c...|topic_0|        3|     3|2026-01-03 07:14:...|            0|
# | 3241|{"order_id":27,"c...|topic_0|        2|     3|2026-01-03 07:14:...|            0|
# | 4189|{"order_id":34,"c...|topic_0|        2|     4|2026-01-03 07:14:...|            0|
# | 8136|{"order_id":41,"c...|topic_0|        2|     5|2026-01-03 07:14:...|            0|
# | 7776|{"order_id":43,"c...|topic_0|        2|     6|2026-01-03 07:14:...|            0|
# | 4367|{"order_id":23,"c...|topic_0|        1|     5|2026-01-03 07:14:...|            0|
# +-----+--------------------+-------+---------+------+--------------------+-------------+
# only showing top 20 rows


%fs
ls /Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_checkpoint/

path	name	size	modificationTime
dbfs:/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_checkpoint/ __tmp_path_dir/	__tmp_path_dir/ 	0	1767423408000
dbfs:/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_checkpoint/commits/	commits/	        0	1767423492000
dbfs:/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_checkpoint/metadata	metadata	        45	1767423408000
dbfs:/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_checkpoint/offsets/	offsets/	        0	1767423492000
dbfs:/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_checkpoint/sources/	sources/        	0	1767423492000



# re-insert the data again: order_input.json

# trigger kafka batch job.

# create API key and secret in confluent cloud once after creating a cluster.
# then attatch the API key and secret to the topic you create in confluent cloud.
# also check the bootstrap servers.

# Install this libraries in databricks.
# here: https://adb-7405611670894568.8.azuredatabricks.net/compute/clusters/1231-074055-14oqu1ej/libraries?o=7405611670894568
# confluent-kafka[avro,json,protobuf]>=1.4.2

# run this code in databricks.


from confluent_kafka import Producer

import socket, json

# https://confluent.cloud/environments/env-v8jrqz/clusters/lkc-mo88oq/settings/kafka
conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '2JYEF54HVRAGH62C',
        'sasl.password': 'cfltlPlBW6OzUBDjF6opu4WA08WOPzixKNfNFY+55pgKwCreRSNz50om4sAbmoGQ',
        'client.id': 'ccloud-python-client-c0aeab10-5a58-4ce7-9bbb-e9f175ef853d'}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_data/order_input.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = 'topic_0', key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()


spark.sql("select count(*) from misgauravorderstablenew").show()


# re-insert the data again: order_input_new.json

# trigger kafka batch job.

# create API key and secret in confluent cloud once after creating a cluster.
# then attatch the API key and secret to the topic you create in confluent cloud.
# also check the bootstrap servers.

# Install this libraries in databricks.
# here: https://adb-7405611670894568.8.azuredatabricks.net/compute/clusters/1231-074055-14oqu1ej/libraries?o=7405611670894568
# confluent-kafka[avro,json,protobuf]>=1.4.2

# run this code in databricks.


from confluent_kafka import Producer

import socket, json

# https://confluent.cloud/environments/env-v8jrqz/clusters/lkc-mo88oq/settings/kafka
conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '2JYEF54HVRAGH62C',
        'sasl.password': 'cfltlPlBW6OzUBDjF6opu4WA08WOPzixKNfNFY+55pgKwCreRSNz50om4sAbmoGQ',
        'client.id': 'ccloud-python-client-c0aeab10-5a58-4ce7-9bbb-e9f175ef853d'}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print('faied to deliver msg: %s: %s' % (str(msg), str(msg)))
    else:
        print('msg produced: %s' % (str(msg)))
        print(f'msg produced key in binary is: {msg.key()} & msg produced value in binary is {msg.value()}')
        print(f'msg produced key in string is: {msg.key()} & msg produced value in binary is {msg.value()}')

with open('/Volumes/misgaurav_databricks_ws_7405615643942288/default/misgaurav_v/retail_data/orders_data/order_input_new.json', mode= 'r' ) as files:
    for line in files:
        order = json.loads(line)
        customer_id = str(order['customer_id'])
        producer.produce(topic = 'topic_0', key = customer_id, value = line, callback = acked)
        producer.poll(1)
        producer.flush()

# order_input_new.json file contains 2 rows
spark.sql("select count(*) from misgauravorderstablenew").show()

%sql
use catalog misgaurav_databricks_ws_7405615643942288;
use schema default;
    
show tables;
-- database	tableName	              isTemporary
-- default	  misgauravorderstablenew	false
-- 	        _sqldf	                true



