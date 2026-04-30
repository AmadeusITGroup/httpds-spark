package com.amadeus.spark.datasource.remote.metrics

import org.apache.spark.sql.connector.metric.CustomMetric

/**
 * Custom metric that reports the minimum number of files downloaded by any partition.
 *
 * This metric helps monitor download distribution across partitions and can be useful
 * for optimizing partition sizes.
 */
class MinFilesDownloadedMetric extends CustomMetric {

  /**
   * Returns the name of the metric as displayed in Spark UI.
   */
  override def name(): String = "min files downloaded"

  /**
   * Returns the description of the metric.
   */
  override def description(): String = "Minimum number of files downloaded by any single partition"

  /**
   * Aggregates metric values from all tasks.
   *
   * For min files downloaded, we take the minimum value across all partitions.
   *
   * @param taskMetrics array of metric values from each task/partition
   * @return the aggregated metric value as a string
   */
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    if (taskMetrics.isEmpty) {
      return NA
    }

    taskMetrics.min.toString
  }
}
