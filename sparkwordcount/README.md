Spark-Shell scala code

var file = sc.textFile("hdfs://nn1:8020/user/hduser/inputText/*")
var rdd = file.flatMap(line => line.replaceAll("[^a-zA-Z ]","").toUpperCase.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)
rdd.saveAsTextFile("hdfs://nn1:8020/user/hduser/output/")

Compilation and launch on Linux

mvn clean && mvn compile && mvn package

$SPARK_HOME/bin/spark-submit \
--class com.testhadoop.sparkwordcount.SparkWordCount \
sparkwordcount-0.1.jar \
{spark://nn1:7077 or local[Number of threats]} {HDFS or local} /user/hduser/inputText/* /user/hduser/output/
