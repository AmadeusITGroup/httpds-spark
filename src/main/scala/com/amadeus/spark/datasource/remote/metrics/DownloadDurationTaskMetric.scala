package com.amadeus.spark.datasource.remote.metrics

import com.amadeus.spark.datasource.remote.metrics.CustomTaskMetricType.CustomTaskMetricType
import org.apache.spark.sql.connector.metric.CustomTaskMetric

/**
 * Custom task metric that reports the download duration (in milliseconds) for a specific task/partition.
 *
 * @param durationMs the download duration in milliseconds for this task
 * @param metricType the type of duration metric (e.g., avg, max, min)
 */
case class DownloadDurationTaskMetric(durationMs: Long, metricType: CustomTaskMetricType) extends CustomTaskMetric {

  /**
   * Returns the name of the metric (must match the CustomMetric name).
   */
  override def name(): String = s"$metricType download duration"

  /**
   * Returns the value of this task's metric.
   */
  override def value(): Long = durationMs
}
