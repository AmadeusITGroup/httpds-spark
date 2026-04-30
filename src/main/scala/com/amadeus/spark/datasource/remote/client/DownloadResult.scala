package com.amadeus.spark.datasource.remote.client

/**
 * Represents the result of a file download operation.
 */
sealed trait DownloadResult {
  def durationMs: Option[Long]
}

/**
 * Represents a successful file download.
 *
 * @param lines   iterator over the downloaded file lines
 * @param metrics metrics related to the download operation
 * @param durationMs time taken to download in milliseconds (only populated when debug logging is enabled)
 */
case class DownloadSuccess(lines: Iterator[RawFileLine], metrics: DownloadMetrics, durationMs: Option[Long] = None) extends DownloadResult

/**
 * Represents a failed file download.
 *
 * @param error the error line that caused the failure
 */
case class DownloadFailure(error: RawFileLine, durationMs: Option[Long] = None) extends DownloadResult
