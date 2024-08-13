# spark
# sqlContext
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
    
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
import re
import pandas as pd
m = re.finditer(r'.*?(spark).*?', "I'm searching for a spark in PySpark", re.I)
for match in m:
    print(match, match.start(), match.end())
import glob

raw_data_files = glob.glob('*.gz')
raw_data_files
base_df = spark.read.text(raw_data_files)
base_df.printSchema()
type(base_df)
base_df_rdd = base_df.rdd
type(base_df_rdd)
base_df.show(10, truncate=False)
base_df_rdd.take(10)
print((base_df.count(), len(base_df.columns)))
sample_logs = [item['value'] for item in base_df.take(15)]
sample_logs
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1)
           if re.search(host_pattern, item)
           else 'no match'
           for item in sample_logs]
hosts
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]
timestamps

method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups()
               if re.search(method_uri_protocol_pattern, item)
               else 'no match'
              for item in sample_logs]
method_uri_protocol
status_pattern = r'\s(\d{3})\s'
status = [re.search(status_pattern, item).group(1) for item in sample_logs]
print(status)
content_size_pattern = r'\s(\d+)$'
content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]
print(content_size)
from pyspark.sql.functions import regexp_extract

logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
logs_df.show(10, truncate=True)
print((logs_df.count(), len(logs_df.columns)))
(base_df.filter(base_df['value'].isNull()).count())
bad_rows_df = logs_df.filter(logs_df['host'].isNull()| 
                             logs_df['timestamp'].isNull() | 
                             logs_df['method'].isNull() |
                             logs_df['endpoint'].isNull() |
                             logs_df['status'].isNull() |
                             logs_df['content_size'].isNull()|
                             logs_df['protocol'].isNull())
bad_rows_df.count()
logs_df.columns
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

# Build up a list of column expressions, one per column.
exprs = [count_null(col_name) for col_name in logs_df.columns]

# Run the aggregation. The *exprs converts the list of expressions into
# variable function arguments.
logs_df.agg(*exprs).show()
null_status_df = base_df.filter(~base_df['value'].rlike(r'\s(\d{3})\s'))
null_status_df.count()
null_status_df.show(truncate=False)
bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                      regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                                      regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                      regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                      regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                      regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                      regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
bad_status_df.show(truncate=False)
logs_df.count()
logs_df = logs_df[logs_df['status'].isNotNull()] 
logs_df.count()
exprs = [count_null(col_name) for col_name in logs_df.columns]
logs_df.agg(*exprs).show()
null_content_size_df = base_df.filter(~base_df['value'].rlike(r'\s\d+$'))
null_content_size_df.count()
null_content_size_df.take(10)    
logs_df = logs_df.na.fill({'content_size': 0})
exprs = [count_null(col_name) for col_name in logs_df.columns]
logs_df.agg(*exprs).show()
from pyspark.sql.functions import udf

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOT: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(int(text[7:11]), month_map[text[3:6]], int(text[0:2]), int(text[12:14]), int(text[15:17]), int(text[18:20]))

sample_ts = [item['timestamp'] for item in logs_df.select('timestamp').take(5)]
sample_ts

[parse_clf_time(item) for item in sample_ts]
udf_parse_time = udf(parse_clf_time)

logs_df = logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')
logs_df.show(10, truncate=True)
logs_df.printSchema()
logs_df.limit(5).toPandas()
logs_df.cache()
content_size_summary_df = logs_df.describe(['content_size'])
content_size_summary_df.toPandas()
from pyspark.sql import functions as F

(logs_df.agg(F.min(logs_df['content_size']).alias('min_content_size'),
             F.max(logs_df['content_size']).alias('max_content_size'),
             F.mean(logs_df['content_size']).alias('mean_content_size'),
             F.stddev(logs_df['content_size']).alias('std_content_size'),
             F.count(logs_df['content_size']).alias('count_content_size')).toPandas())
status_freq_df = (logs_df
                     .groupBy('status')
                     .count()
                     .sort('status')
                     .cache())
print('Total distinct HTTP Status Codes:', status_freq_df.count())
status_freq_pd_df = (status_freq_df.toPandas().sort_values(by=['count'],ascending=False))
status_freq_pd_df
# !pip install -U seaborn
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
# %matplotlib inline

sns.catplot(x='status', y='count', data=status_freq_pd_df, kind='bar', order=status_freq_pd_df['status'])
log_freq_df = status_freq_df.withColumn('log(count)', F.log(status_freq_df['count']))
log_freq_df.show()
log_freq_pd_df = (log_freq_df.toPandas().sort_values(by=['log(count)'],ascending=False))
sns.catplot(x='status', y='log(count)', data=log_freq_pd_df, kind='bar', order=status_freq_pd_df['status'])
host_sum_df =(logs_df.groupBy('host').count().sort('count', ascending=False).limit(10))

host_sum_df.show(truncate=False)
host_sum_pd_df = host_sum_df.toPandas()
host_sum_pd_df.iloc[8]['host']
paths_df = (logs_df.groupBy('endpoint').count().sort('count', ascending=False).limit(20))
paths_pd_df = paths_df.toPandas()
paths_pd_df
not200_df = (logs_df.filter(logs_df['status'] != 200))

error_endpoints_freq_df = (not200_df.groupBy('endpoint').count().sort('count', ascending=False).limit(10))
error_endpoints_freq_df.show(truncate=False)
unique_host_count = (logs_df.select('host').distinct().count())
unique_host_count
host_day_df = logs_df.select(logs_df.host, F.dayofmonth('time').alias('day'))
host_day_df.show(5, truncate=False)
host_day_distinct_df = (host_day_df.dropDuplicates())
host_day_distinct_df.show(5, truncate=False)
# def_mr = pd.get_option('max_rows')
# pd.set_option('max_rows', 10)
def_mr = pd.options.display.max_rows = 10

daily_hosts_df = (host_day_distinct_df.groupBy('day').count().sort("day"))

daily_hosts_df = daily_hosts_df.toPandas()
daily_hosts_df
c = sns.catplot(x='day', y='count', data=daily_hosts_df, kind='point', height=5, aspect=1.5)
daily_hosts_df = host_day_distinct_df.groupBy('day').count().select(col("day"), col("count").alias("total_hosts"))

total_daily_reqests_df = logs_df.select(F.dayofmonth("time").alias("day")).groupBy("day").count().select(col("day"), col("count").alias("total_reqs"))

avg_daily_reqests_per_host_df = total_daily_reqests_df.join(daily_hosts_df, 'day')
avg_daily_reqests_per_host_df = avg_daily_reqests_per_host_df.withColumn('avg_reqs', col('total_reqs') / col('total_hosts')).sort("day")
avg_daily_reqests_per_host_df = avg_daily_reqests_per_host_df.toPandas()
c = sns.catplot(x='day', y='avg_reqs', data=avg_daily_reqests_per_host_df, kind='point', height=5, aspect=1.5)
not_found_df = logs_df.filter(logs_df["status"] == 404).cache()
print(('Total 404 responses: {}').format(not_found_df.count()))
endpoints_404_count_df = (not_found_df.groupBy("endpoint").count().sort("count", ascending=False).limit(20))

endpoints_404_count_df.show(truncate=False)
hosts_404_count_df = (not_found_df.groupBy("host").count().sort("count", ascending=False).limit(20))

hosts_404_count_df.show(truncate=False)
errors_by_date_sorted_df = (not_found_df.groupBy(F.dayofmonth('time').alias('day')).count().sort("day"))

errors_by_date_sorted_pd_df = errors_by_date_sorted_df.toPandas()
errors_by_date_sorted_pd_df
c = sns.catplot(x='day', y='count', data=errors_by_date_sorted_pd_df, kind='point', height=5, aspect=1.5)
(errors_by_date_sorted_df.sort("count", ascending=False).show(3))
hourly_avg_errors_sorted_df = (not_found_df.groupBy(F.hour('time').alias('hour')).count().sort('hour'))
hourly_avg_errors_sorted_pd_df = hourly_avg_errors_sorted_df.toPandas()
c = sns.catplot(x='hour', y='count', data=hourly_avg_errors_sorted_pd_df, kind='bar', height=5, aspect=1.5)

# pd.set_option('max_rows', def_mr)
pd.options.display.max_rows = 10