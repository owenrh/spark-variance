package com.dataflow.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.Random

object VarianceStructuredApp extends VarianceMixins {

  @transient lazy private val log = LoggerFactory.getLogger(getClass.getName)

  @transient lazy private val isStraggler = {
    val isStraggler = System.getenv("STRAGGLER")
    if (isStraggler == null)
      false
    else
      true
  }

  object StreamingMode {
    val Continuous = "CONTINUOUS"
    val Default = "DEFAULT"
  }

  def main(args: Array[String]): Unit = {
    implicit val sc = new SparkContext(new SparkConf())

    val spark = SparkSession.builder.getOrCreate()

    import spark.implicits._

    val mode = appConfig("mode")
    val streamInterval = appConfig("stream.interval").toInt
    val numEventsInInterval = appConfig("stream.numEventsInInterval").toInt

    val microBatchMilliseconds = appConfig("microBatchMilliseconds").toLong

    val numPartitions = getNumberOfPartitions()

    val numMin = appConfig("nums.min").toInt
    val numRange = appConfig("nums.range").toInt

    val streamingMode = appConfig("streamingMode")

    val randomNums = spark
      .readStream
      .format("rate") // <-- use RateStreamSource
      .option("rowsPerSecond", (1000 / streamInterval) * numEventsInInterval)
      .option("numPartitions", numPartitions) // not here, because we want to it be comparable across num partitions
      .load()
      .map(r => numMin + Random.nextInt(numRange))
//      .repartition(numPartitions) // used for non-continuous mode

    val processedStream = mode match {
      case "MINIMAL_VARIANCE" =>
        randomNums
          .mapPartitions(partition => {
            partition.map(num => {
              sleep(num)
              getRateTracker().mark()
              num
            })
          })

      case "STRAGGLER" =>
        val waitRange = appConfig("straggler.waitRange").toInt
        val waitMillis = appConfig("straggler.waitMillis").toInt

        var toProcess = 0

        def resetToProcess(): Unit = {
          toProcess = Random.nextInt(waitRange)
        }

        def timeToWait(): Boolean = {
          toProcess -= 1
          (toProcess <= 0)
        }

        randomNums
          .mapPartitions(partition => {
            partition.map(num => {
              if (isStraggler && timeToWait()) {
                resetToProcess()
                sleep(waitMillis)
              }
              else {
                sleep(num)
              }

              getRateTracker().mark()
              num
            })
          })

      case _ =>
        log.error(s"Failed to recognise mode - $mode")
        throw new RuntimeException("Invalid mode")
    }

    val sunkStream = processedStream
      .writeStream
      .format("console")

    val triggeredStream = streamingMode match {
      case StreamingMode.Default =>
        sunkStream.trigger(Trigger.ProcessingTime(microBatchMilliseconds))
      case StreamingMode.Continuous =>
        sunkStream.trigger(Trigger.Continuous(microBatchMilliseconds))
      case _ =>
        throw new RuntimeException(s"Invalid streaming mode $streamingMode")
    }

    triggeredStream
      .start()
      .awaitTermination()
  }
}
