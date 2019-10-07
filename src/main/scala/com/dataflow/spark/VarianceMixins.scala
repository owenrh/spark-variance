package com.dataflow.spark

import com.codahale.metrics.Meter
import org.apache.spark.metrics.source.VarianceMetricsSource
import org.apache.spark.{SparkContext, SparkEnv}
import org.slf4j.LoggerFactory

trait VarianceMixins {

  @transient lazy private val log = LoggerFactory.getLogger(getClass.getName)

  var metricSource: Option[VarianceMetricsSource] = None

  def getRateTracker(): Meter = {
    if (metricSource == None) {
      metricSource = Some(SparkEnv.get.metricsSystem.getSourcesByName("VarianceSource")(0).asInstanceOf[VarianceMetricsSource])
    }

    metricSource.get.rateTracker
  }

  def sleep(delay: Int): Unit = {
    try {
      Thread.sleep(delay.toLong)
    }
    catch {
      case _ => // ignore
        log.warn("Sleep failed!!!!!")
    }
  }

  def appConfig(propertyName: String)(implicit sc: SparkContext): String = {
    val appKey = sc.getConf.get("spark.app.configKey")
    val configVal = sc.getConf.get(s"spark.$appKey.$propertyName")
    log.info(s"Configuration - spark.$appKey.$propertyName configured as $configVal")
    configVal
  }

  def getNumberOfPartitions(nonPartitionCores: Int = 0)(implicit  sc: SparkContext): Int = {
    val executorCores = sc.getConf.get("spark.executor.cores").toInt
    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numPartitionsPerCore = appConfig("numPartitionsPerCore").toInt
    ((executorCores * numExecutors) - nonPartitionCores) * numPartitionsPerCore
  }
}
