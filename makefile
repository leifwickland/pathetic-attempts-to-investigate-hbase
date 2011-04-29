CLASSPATH=/hadoop/hadoop-core-0.20.2-cdh3u0.jar:/hbase/hbase-0.90.1-cdh3u0.jar:/hadoop/lib/*:/hbase/lib/*:/scala-json/target/scala_2.8.1/json_2.8.1-2.1.6-SNAPSHOT.jar

INPUT_JSON=/home/json/week.json

all: hbaseactor

run: runhbaseactor

hbaseactor: HBaseActor.class

runhbaseactor: hbaseactor 
	scala -classpath .:$(CLASSPATH) HBaseActor $(INPUT_JSON)

HBaseActor.class: HBaseActor.scala
	scalac  -classpath $(CLASSPATH) HBaseActor.scala

hbaseclient: HBaseClient.class 

runhbaseclient: hbaseclient
	scala -classpath .:$(CLASSPATH) HBaseClient $(INPUT_JSON)

HBaseClient.class: HBaseClient.scala
	scalac  -classpath $(CLASSPATH) HBaseClient.scala

hbaserand: HBaseRand.class 

runhbaserand: hbaserand
	scala -classpath .:$(CLASSPATH) HBaseRand $(INPUT_JSON)

HBaseRand.class: HBaseRand.scala
	scalac  -classpath $(CLASSPATH) HBaseRand.scala

clean:
	rm *.class
