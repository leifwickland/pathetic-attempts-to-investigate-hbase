CLASSPATH=/hadoop/hadoop-core-0.20.2-cdh3u0.jar:/hbase/hbase-0.90.1-cdh3u0.jar:/hadoop/lib/*:/hbase/lib/*:/scala-json/target/scala_2.8.1/json_2.8.1-2.1.6-SNAPSHOT.jar

INPUT_JSON=/home/json/week.json
SCALAC=scalac -classpath $(CLASSPATH)
SCALA=scala -classpath .:$(CLASSPATH)

all: hbaseactor

run: runhbaseactor

hbaseactor: HBaseActor.class

runhbaseactor: hbaseactor 
	$(SCALA) HBaseActor $(INPUT_JSON)

HBaseActor.class: HBaseActor.scala
	$(SCALAC) HBaseActor.scala

hbaseclient: HBaseClient.class 

runhbaseclient: hbaseclient
	$(SCALA) HBaseClient $(INPUT_JSON)

HBaseClient.class: HBaseClient.scala
	$(SCALAC) HBaseClient.scala

hbaserand: HBaseRand.class 

runhbaserand: hbaserand
	$(SCALA) HBaseRand $(INPUT_JSON)

HBaseRand.class: HBaseRand.scala
	$(SCALAC) HBaseRand.scala

clean:
	rm *.class
