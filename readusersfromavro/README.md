Generated dataset using SQOOP(partitioned by Year)

	hdfs dfs -mkdir /user/hive/warehouse/hadoop_test_users_avro
	
	sqoop import --connect jdbc:mysql://nn1:3306/test_hadoop \
	--username hadoopuser --P \
	-e "SELECT id, username FROM users WHERE YEAR(regdate)='2011' AND \$CONDITIONS" \
	--split-by id \
	--fields-terminated-by ',' \
	--target-dir /user/hive/warehouse/hadoop_test_users_avro/regyear=2011 \
	--delete-target-dir \
	--as-avrodatafile \
	--compress \
	--compression-codec deflate \
	--class-name User
	
	sqoop import --connect jdbc:mysql://nn1:3306/test_hadoop \
	--username hadoopuser --P \
	-e "SELECT id, username FROM users WHERE YEAR(regdate)='2012' AND \$CONDITIONS" \
	--split-by id \
	--fields-terminated-by ',' \
	--target-dir /user/hive/warehouse/hadoop_test_users_avro/regyear=2012 \
	--delete-target-dir \
	--as-avrodatafile \
	--compress \
	--compression-codec deflate \
	--class-name User

Add SQOOP generated avro schema and serialized class to project (User.avsc and User.java)

mvn clean && mvn compile && mvn package

hadoop jar readusersfromavro-0.1.jar ReadUsersFromAvro /user/hive/warehouse/hadoop_test_users_avro/regyear=2011 /user/hive/warehouse/hadoop_test_users_avro/regyear=2012 /user/hduser/output /user/hduser/User.avsc
