package com.amadeus.spark.datasource.remote.client

/**
 * Represents metrics related to a file download operation.
 *
 * @param lineCount number of lines downloaded
 * @param byteCount total number of bytes downloaded
 */
case class DownloadMetrics(lineCount: Int, byteCount: Long)
