commands to run program


Map Reduce

1. ssh localhost
2. hdfs namenode -format
3. start-all.sh
4. jps //  to check if datanode exist
5. hdfs dfs -mkdir /MR
6. hdfs dfs -copyFromLocal data_16GB.txt /MR
7. hdfs dfs -ls /MR
8. chmod 777 mapper.py reducer.py
9. hadoop jar hadoop-streaming-3.3.5.jar -input /MR/data_16GB.txt -output /MR/Output -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file mapper.py -file reducer.py
10. hdfs dfs -cat /MR/Output/part-00000


Map-Reduce with Combiner
1. ssh localhost
2. hdfs namenode -format
3. start-all.sh
4. jps //  to check if datanode exist
5. hdfs dfs -mkdir /MR
6. hdfs dfs -copyFromLocal data_16GB.txt /MR
7. hdfs dfs -ls /MR
8. chmod 777 mapper.py reducer.py
9. hadoop jar hadoop-streaming-3.3.5.jar -input /MR/data_16GB.txt -output /MR/Output -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file mapper.py -file reducer.py -combiner "python3 Reducer.py"
10. hdfs dfs -cat /MR/Output/part-00000


Map-Reduce with Partitioner 
1. ssh localhost
2. hdfs namenode -format
3. start-all.sh
4. jps //  to check if datanode exist
5. hdfs dfs -mkdir /MR
6. hdfs dfs -copyFromLocal data_16GB.txt /MR
7. hdfs dfs -ls /MR
8. chmod 777 mapper.py reducer.py
9. hadoop jar hadoop-streaming-3.3.5.jar -input /MR/data_16GB.txt -output /MR/Output -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file mapper.py -file reducer.py -combiner "python3 Reducer.py" -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner -D mapreduce.partition.keypartitioner.options="-k1,2" 
10. hdfs dfs -cat /MR/Output/part-00000


Map-Reduce with Compression
1. ssh localhost
2. hdfs namenode -format
3. start-all.sh
4. jps //  to check if datanode exist
5. hdfs dfs -mkdir /MR
6. hdfs dfs -copyFromLocal data_16GB.txt /MR
7. hdfs dfs -ls /MR
8. chmod 777 mapper.py reducer.py
9. hadoop jar hadoop-streaming-3.3.5.jar -input /MR/data_16GB.txt -output /MR/Output -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file mapper.py -file reducer.py -combiner "python3 Reducer.py" -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec -D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec -input /MR/data_16GB.txt \
10. hdfs dfs -cat /MR/Output/part-00000
