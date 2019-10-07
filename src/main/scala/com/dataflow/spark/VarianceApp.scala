package com.dataflow.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.Random

object VarianceApp extends VarianceMixins {

  @transient lazy private val log = LoggerFactory.getLogger(getClass.getName)

  @transient lazy private val isStraggler = {
    val isStraggler = System.getenv("STRAGGLER")
    if (isStraggler == null)
      false
    else
      true
  }

  def main(args: Array[String]): Unit = {
    implicit val sc = new SparkContext(new SparkConf())

    val mode = appConfig("mode")
    val streamInterval = appConfig("stream.interval").toInt
    val numEventsInInterval = appConfig("stream.numEventsInInterval").toInt

    val microBatchMilliseconds = appConfig("microBatchMilliseconds").toLong

    val numPartitions = getNumberOfPartitions(0)

    val numMin = appConfig("nums.min").toInt
    val numRange = appConfig("nums.range").toInt

    val ssc = new StreamingContext(sc, Milliseconds(microBatchMilliseconds))

    def getIntDStream(): DStream[Int] = {
      ssc.receiverStream(new RandomIntStreamReceiver(numMin, numRange, streamInterval, numEventsInInterval, StorageLevel.MEMORY_ONLY))
        .repartition(numPartitions)
    }

    val processedStream = mode match {
      case "MINIMAL_VARIANCE" =>
        getIntDStream()
          .mapPartitions(partition => {
            val meter = getRateTracker()
            partition.map(num => {
              sleep(num)
              meter.mark()
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

        getIntDStream()
          .mapPartitions(partition => {
            val meter = getRateTracker()
            partition.map(num => {
              if (isStraggler && timeToWait()) {
                resetToProcess()
                sleep(waitMillis)
              }
              else {
                sleep(num)
              }
              meter.mark()
              num
            })
          })

      case _ =>
        log.error(s"Failed to recognise mode - $mode")
        throw new RuntimeException("Invalid mode")
    }

    processedStream
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          if (partition.nonEmpty) {
            log.info(s"Partition complete - ${partition.length}")
          }
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
