package org.apache.spark.metrics.source

import com.codahale.metrics.{Histogram, Meter, MetricRegistry}

class VarianceMetricsSource extends Source {
  override val sourceName: String = "VarianceSource"
  override val metricRegistry = new MetricRegistry

  val rateTracker: Meter = metricRegistry.meter(MetricRegistry.name("processingRate"))
}
