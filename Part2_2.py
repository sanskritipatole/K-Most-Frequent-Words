from pyspark.sql import functions as F

# Selecting the required columns and adding new columns for Year and DayOfWeek
logs_new = logs_df.select(logs_df.host, logs_df.method, logs_df.endpoint, logs_df.protocol,
                          logs_df.status, logs_df.content_size, logs_df.time, F.year('time').alias('Year'),
                          date_format("time", "EEEE").alias('DayOfWeek'))

# Displaying the first 5 rows of the DataFrame and printing the schema
logs_new.show(5, truncate=False)
logs_new.printSchema()

# Counting the number of unique Years
unique_years = logs_new.select('Year').distinct().count()
print(unique_years)

'''
unique_Year = (logs_new
                     .groupby(['DayOfWeek','endpoint'])
                     .count()
                     .max()
                     .sort('count', ascending=False)
                     .limit(5)
                    )
'''

# Grouping by DayOfWeek and endpoint, counting the occurrences, and finding the maximum count for each DayOfWeek
unique_year = (logs_new.groupby(['DayOfWeek', 'endpoint'])
               .count()
               .orderBy(F.desc('count'))
               .groupBy('DayOfWeek')
               .agg({'count': 'max', 'endpoint': 'first'})
               .orderBy(F.desc('max(count)'))
               .limit(10)
              )

# Converting the result to a Pandas DataFrame
unique_year = unique_year.toPandas()
print(unique_year)

# Filtering logs with status 404
ne_404_logs = logs_new.filter(logs_new['status'] == 404)

# Counting the occurrences of error endpoints by Year
error_endpoints_freq_df = ne_404_logs.groupby('Year').count()

error_endpoints_freq_df.show(truncate=False)