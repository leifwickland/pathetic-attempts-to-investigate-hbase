CLASSPATH=/hadoop/hadoop-core-0.20.2-cdh3u0.jar:/hbase/hbase-0.90.1-cdh3u0.jar:/hadoop/lib/*:/hbase/lib/*:/scala-json/target/scala_2.8.1/json_2.8.1-2.1.6-SNAPSHOT.jar

INPUT_JSON=/home/json/week.json
SCALAC=scalac -classpath $(CLASSPATH)
SCALA=scala -classpath .:$(CLASSPATH)

all: HBaseActor

run: runHBaseActor

HBaseActor: HBaseActor.class

runHBaseActor: HBaseActor 
	$(SCALA) HBaseActor $(INPUT_JSON)

HBaseClient: HBaseClient.class 

runHBaseClient: HBaseClient
	$(SCALA) HBaseClient $(INPUT_JSON)

HBaseRand: HBaseRand.class 

runHBaseRand: HBaseRand
	$(SCALA) HBaseRand $(INPUT_JSON)

%.class: %.scala
	$(SCALAC) $<

clean:
	rm *.class
