package com.amadeus.spark.datasource.remote

import scala.concurrent.duration.Duration

package object metrics {

  /** String used to represent "Not Available" metric values. */
  private[metrics] val NA = "N/A"

  /**
   * Formats duration into a human-readable string.
   */
  private[metrics] def formatDuration(durationMs: Long): String = {
    if (durationMs == 0) {
      "0 millisecond"
    } else {
      Duration(s"$durationMs ms").toString
    }
  }

  /**
   * Formats record throughput into a human-readable string.
   */
  private[metrics] def formatRecordThroughput(recordsThroughput: Long): String = f"$recordsThroughput%,d records/sec"

  /**
   * Formats byte throughput into a human-readable string in MB/s.
   */
  private[metrics] def formatBytesThroughput(bytesThroughput: Long): String = {
    val mbPerSec = bytesThroughput / (1024.0 * 1024.0)
    f"$mbPerSec%.2f MB/s"
  }
}
