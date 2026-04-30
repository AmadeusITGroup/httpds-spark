package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the average number of files downloaded by any partition.
 *
 * This metric helps monitor download distribution across partitions and can be useful
 * for optimizing partition sizes.
 */
class AvgFilesDownloadedMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "avg files downloaded"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Average number of files downloaded by any single partition"

  /**
   * Aggregates metric values from all tasks.
   *
   * For avg files downloaded, we take the average value across all partitions.
   *
   * @param taskMetrics array of metric values from each task/partition
   * @return the aggregated metric value as a human-readable string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      return NA
    }

    val avgDownloadedFiles = taskMetrics.sum / taskMetrics.length
    f"$avgDownloadedFiles%,d"
  }
}
