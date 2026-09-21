package com.amadeus.spark.datasource.remote.metrics

import com.amadeus.spark.datasource.remote.metrics.CustomTaskMetricType.CustomTaskMetricType
import org.apache.spark.sql.connector.metric.CustomTaskMetric

/**
 * Custom task metric that reports the records throughput (records per second) for a specific task/partition.
 *
 * @param recordsPerSecond the records throughput for this task
 * @param metricType    the type of throughput metric (e.g., avg, max, min)
 */
case class RecordsThroughputTaskMetric(recordsPerSecond: Long, metricType: CustomTaskMetricType) extends CustomTaskMetric {

  /**
   * Returns the name of the metric (must match the CustomMetric name).
   */
  override def name(): String = s"$metricType records throughput"

  /**
   * Returns the value of this task's metric.
   */
  override def value(): Long = recordsPerSecond
}
