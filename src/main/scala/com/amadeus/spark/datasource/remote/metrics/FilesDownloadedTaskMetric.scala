package com.amadeus.spark.datasource.remote.metrics

import com.amadeus.spark.datasource.remote.metrics.CustomTaskMetricType.CustomTaskMetricType
import org.apache.spark.sql.connector.metric.CustomTaskMetric

/**
 * Custom task metric that reports the number of files downloaded for a specific task/partition.
 *
 * @param filesDownloaded the total number of files downloaded for this task
 * @param metricType     the type of metric (e.g., max)
 */
case class FilesDownloadedTaskMetric(filesDownloaded: Long, metricType: CustomTaskMetricType) extends CustomTaskMetric {

  /**
   * Returns the name of the metric.
   */
  override def name(): String = s"$metricType files downloaded"

  /**
   * Returns the value of this task's metric.
   */
  override def value(): Long = filesDownloaded
}
