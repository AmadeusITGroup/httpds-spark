package com.amadeus.spark.datasource.remote.helpers

import com.amadeus.spark.datasource.remote.metrics._
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}

/**
 * Helper trait for tracking and building custom metrics related to remote file processing.
 */
trait MetricsHelper {

  /** Start time for metrics. */
  private val startTimeMs: Long = System.currentTimeMillis()

  /** Number of records read. */
  // Mutable accumulator required for streaming metrics tracking
  // scalafix:off DisableSyntax.var
  protected var recordsRead: Long = 0

  /** Total bytes downloaded across all files. */
  protected var totalBytesDownloaded: Long = 0

  /** Number of files processed. */
  protected var filesProcessed: Int = 0
  // scalafix:on DisableSyntax.var

  protected def buildMetrics(): Array[CustomTaskMetric] = {
    val durationMs    = System.currentTimeMillis() - startTimeMs
    val recordsPerSec = if (durationMs > 0) (recordsRead * 1000) / durationMs else 0
    val bytesPerSec   = if (durationMs > 0) (totalBytesDownloaded * 1000) / durationMs else 0

    Array(
      FilesDownloadedTaskMetric(filesProcessed.toLong, CustomTaskMetricType.Avg),
      FilesDownloadedTaskMetric(filesProcessed.toLong, CustomTaskMetricType.Max),
      FilesDownloadedTaskMetric(filesProcessed.toLong, CustomTaskMetricType.Min),
      BytesDownloadedTaskMetric(totalBytesDownloaded),
      DownloadDurationTaskMetric(durationMs, CustomTaskMetricType.Avg),
      DownloadDurationTaskMetric(durationMs, CustomTaskMetricType.Max),
      DownloadDurationTaskMetric(durationMs, CustomTaskMetricType.Min),
      RecordsThroughputTaskMetric(recordsPerSec, CustomTaskMetricType.Avg),
      RecordsThroughputTaskMetric(recordsPerSec, CustomTaskMetricType.Max),
      RecordsThroughputTaskMetric(recordsPerSec, CustomTaskMetricType.Min),
      BytesThroughputTaskMetric(bytesPerSec, CustomTaskMetricType.Avg),
      BytesThroughputTaskMetric(bytesPerSec, CustomTaskMetricType.Max),
      BytesThroughputTaskMetric(bytesPerSec, CustomTaskMetricType.Min)
    )
  }
}

object MetricsHelper {

  /**
   * Defines the supported custom metrics for this data source.
   *
   * @return an array of CustomMetric instances representing the metrics that can be tracked and reported.
   */
  def supportedMetrics(): Array[CustomMetric] = {
    Array(
      new MaxFilesDownloadedMetric(),
      new AvgFilesDownloadedMetric(),
      new MinFilesDownloadedMetric(),
      new TotalBytesDownloadedMetric(),
      new AvgDownloadDurationMetric(),
      new MaxDownloadDurationMetric(),
      new MinDownloadDurationMetric(),
      new AvgRecordsThroughputMetric(),
      new MaxRecordsThroughputMetric(),
      new MinRecordsThroughputMetric(),
      new AvgBytesThroughputMetric(),
      new MaxBytesThroughputMetric(),
      new MinBytesThroughputMetric()
    )
  }
}
