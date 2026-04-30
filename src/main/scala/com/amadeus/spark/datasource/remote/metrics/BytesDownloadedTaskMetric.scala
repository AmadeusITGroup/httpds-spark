package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomTaskMetric

/**
 * Custom task metric that reports the total bytes downloaded for a specific task/partition.
 *
 * @param bytesDownloaded the total number of bytes downloaded for this task
 */
case class BytesDownloadedTaskMetric(bytesDownloaded: Long) extends CustomTaskMetric {

  /**
   * Returns the name of the metric (must match the CustomMetric name).
   */
  override def name(): String = "total bytes downloaded"

  /**
   * Returns the value of this task's metric.
   */
  override def value(): Long = bytesDownloaded
}
