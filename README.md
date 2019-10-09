### Spark Variance
This project contains Spark job definitions that use synthetic data to create four different backpressure scenarios used in the [Stream Processing Backpressure Smackdown](http://owenrh.me.uk/blog/2019/09/30/).

### Running Spark
There are a number of ways to install and run Spark depending on your available hardware. I am just going to cover running it locally on a Mac laptop.

#### Install Spark
Download the version in question from the [Spark  website](https://spark.apache.org/downloads). I used v2.4.3.

#### Configure and run Spark
Due to the shape of the Spark API, there is not an easy way to create our artificial straggler without the use of an environment variable. So for our Spark runs we have to jump through a few extra steps to get setup.

Firstly, we need to update the `SPARK_HOME/conf/spark-env.sh` along the following lines:

```
SPARK_WORKER_CORES=1
SPARK_WORKER_INSTANCES=4
SPARK_WORKER_MEMORY=1g
```

Next we run up the Spark master:
```
spark-master.sh
```

Then we run up our non-straggling slaves:
```
start-slave.sh spark://mbp.local:7077
```

Then we inject our environment variable:
```
export STRAGGLER=true
```

We modify the `spark-env.sh` config to increase the number of workers by one:

```
SPARK_WORKER_INSTANCES=5
```

Then we run our start slaves command again, which starts our final straggling worker:
```
start-slave.sh spark://mbp.local:7077
```

___Note, for DStreams you will need 5 workers with a core a piece, as a core is reserved for the Stream Receiver. For Structured Streaming you will only need 4 workers, as it doesn't require the extract core. The other point to note for the DStreams setup is that it is non-deterministic, as sometimes the Streaming Receiver will land on the 'straggler' node. It's far from ideal : /___

### Setup Elasticsearch and Kibana for metrics
_TODO_ - if anyone is interested in this raise an issue and I'll sort out the extra info - in the meantime you will need to comment out any lines in the scenario configuration that refer to the metrics configuration or jar file

### Running scenarios
First build this project using `mvn clean package`. Then run the appropriate scenario using:
```
spark-submit --properties-file scenarios/<backpressure scenario>.properties --master spark://mbp.local:7077 --class com.dataflow.spark.VarianceApp target/spark-variance-1.0-SNAPSHOT.jar 
```

Note, for the Structured Streaming/Continuous Processing scenarios the class will need to be changed from `VarianceApp` to `VarianceStructuredApp`.