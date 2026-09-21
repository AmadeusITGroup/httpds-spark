package com.amadeus.spark.datasource.remote.metrics

import com.amadeus.spark.datasource.remote.metrics.CustomTaskMetricType.CustomTaskMetricType
import org.apache.spark.sql.connector.metric.CustomTaskMetric

/**
 * Custom task metric that reports the bytes throughput (bytes per second) for a specific task/partition.
 *
 * @param bytesPerSecond the bytes throughput for this task
 * @param metricType    the type of throughput metric (e.g., avg, max, min)
 */
case class BytesThroughputTaskMetric(bytesPerSecond: Long, metricType: CustomTaskMetricType) extends CustomTaskMetric {

  /**
   * Returns the name of the metric (must match the CustomMetric name).
   */
  override def name(): String = s"$metricType bytes throughput"

  /**
   * Returns the value of this task's metric.
   */
  override def value(): Long = bytesPerSecond
}
