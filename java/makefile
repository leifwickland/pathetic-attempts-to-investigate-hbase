all: HBaseClient.class

HBaseClient.class: HBaseClient.java
	javac -cp /hadoop/hadoop-core-0.20.2-cdh3u0.jar:/hbase/hbase-0.90.1-cdh3u0.jar HBaseClient.java

run:
	java -cp .:/hadoop/hadoop-core-0.20.2-cdh3u0.jar:/hbase/hbase-0.90.1-cdh3u0.jar:/hadoop/lib/*:/hbase/lib/* HBaseClient
