import time
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_extract
import os

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Create a Kafka consumer
consumer = KafkaConsumer(
    'demolog_test',
    bootstrap_servers='localhost:9092',
    group_id='test1'
)

# Create an empty list to store the key-value pairs
records = []
print("Start")
count=0
# Process incoming messages
while True:
    # Start the timer
    start = time.time()

    # Poll messages for a fixed duration (5 seconds in this example)
    received_records = consumer.poll(timeout_ms=5000)

    if received_records:
        # Messages received from the producer
        for partition, rec_list in received_records.items():
            for record in rec_list:
                key = record.key
                value = record.value.decode()  # Decode the bytes to string
                records.append((key, value))
                data_received = True
                print("Getting Data and appending={}".format(count))
                count += 1
        continue
    else:
        print("Producer has no data...")
        schema = StructType([
            StructField("key", StringType(), nullable=True),
            StructField("value", StringType(), nullable=True)
        ])

        # Create a DataFrame from the list of key-value pairs
        my_df = spark.createDataFrame(records, schema)
        # my_df.show()

        # Create a schema for the DataFrame
        # Extract columns using regex patterns
        host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})]'
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        status_pattern = r'\s(\d{3})\s'
        content_size_pattern = r'\s(\d+)$'

        logs_df = my_df.select(
            regexp_extract('value', host_pattern, 1).alias('host'),
            regexp_extract('value', ts_pattern, 1).alias('timestamp'),
            regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
            regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
            regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
            regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
            regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size')
        )

        # logs_df.show(10, truncate=True)
        print((logs_df.count(), len(logs_df.columns)))

        # Preprocessing Starts as that of Part2

        logs_df = logs_df[logs_df['status'].isNotNull()]

        logs_df = logs_df.na.fill({'content_size': 0})

        from pyspark.sql.functions import udf

        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
            'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        def parse_clf_time(text):
            """ Convert Common Log time format into a Python datetime object
            Args:
                text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
            Returns:
                a string suitable for passing to CAST('timestamp')
            """
            # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
            return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
                int(text[7:11]),
                month_map[text[3:6]],
                int(text[0:2]),
                int(text[12:14]),
                int(text[15:17]),
                int(text[18:20])
            )

        udf_parse_time = udf(parse_clf_time)

        logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp'])
                                            .cast('timestamp')
                                            .alias('time'))
                   .drop('timestamp'))

        logs_df.show(10, truncate=True)

        print("Done_Analysis")

        output_path = "file:/Users/sanskriti/Parquet/output19_{}.parquet".format(count)
        logs_df.write.parquet(output_path)
        print("Saving File")

        os.system("hdfs dfs -copyFromLocal /Users/adityakanodia/Parquet/output19_{}.parquet  /word_count_in_python".format(count))
        break

    # Perform further processing on the DataFrame
    # ...

    # Wait for 10 seconds before checking for new data again
    time.sleep(10)

# Close the Kafka consumer connection
consumer.close()
