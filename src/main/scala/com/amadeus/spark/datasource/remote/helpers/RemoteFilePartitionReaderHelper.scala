package com.amadeus.spark.datasource.remote.helpers

import com.amadeus.spark.datasource.remote.RemoteFileInputPartition
import com.amadeus.spark.datasource.remote.client.{DownloadFailure, DownloadResult, DownloadSuccess, RawFileLine}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Abstract helper class for RemoteFilePartitionReader to handle common logic of iterating through files,
 * handling download results, and managing memory cleanup. HTTP retries are the client's responsibility.
 *
 * This class provides a structured way to implement the core functionality of reading from remote files
 * while allowing concrete implementations to focus on the specifics of downloading and client management.
 *
 * Key responsibilities include:
 * - Managing the current file iterator and row state
 * - Handling download results and updating metrics
 * - Cleaning up memory by draining iterators and clearing byte arrays
 * - Providing an abstract method for downloading the next file, which must be implemented by subclasses
 */
abstract class RemoteFilePartitionReaderHelper(schema: StructType, partition: RemoteFileInputPartition) extends PartitionReader[InternalRow] with MetricsHelper with Logging {

  /** Iterator over the current file's lines. */
  // Mutable iteration state required for file-by-file partition reading
  // scalafix:off DisableSyntax.var
  protected var currentIterator: Iterator[RawFileLine] = Iterator.empty

  /** Current row to return. */
  protected var currentRow: InternalRow = _

  /** Number of files failed. */
  protected var filesFailed: Int = 0
  // scalafix:on DisableSyntax.var

  /** List of failed files for logging. */
  protected val failedFiles: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  /** Guards against running the close logic more than once. */
  // Volatile var required for idempotent close() in async/sync reader
  // scalafix:off DisableSyntax.var
  @volatile private var isClosed: Boolean = false
  // scalafix:on DisableSyntax.var

  /**
   * Advances to the next record.
   *
   * This method handles:
   * 1. Iterating through files assigned to this partition
   * 2. Downloading files using the client (which owns any retry logic)
   * 3. Iterating through log lines within each file
   *
   * @return true if there is a next record, false otherwise
   */
  override def next(): Boolean = {
    try {
      // Try to get next record from current iterator
      if (!downloadNextFile()) {
        // scalafix:off DisableSyntax.return
        return false
        // scalafix:on DisableSyntax.return
      }

      val event = currentIterator.next()
      currentRow = event.toInternalRow(schema)
      recordsRead += 1

      // Clear byte arrays from the consumed RawFileLine to release memory
      event.logContent.foreach(arr => util.Arrays.fill(arr, 0.toByte))
      event.rawFileBinary.foreach(arr => util.Arrays.fill(arr, 0.toByte))

      true

    } catch {
      case NonFatal(e) =>
        logError(s"[PARTITION-$partition] Error reading record: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Abstract method to download the next file and populate the current iterator.
   *
   * Implementations should handle downloading the next file in the partition,
   * updating the currentIterator with the file's lines, and returning true if
   * a new file was successfully downloaded or false if there are no more files.
   *
   * @return true if a new file was downloaded and currentIterator is updated, false if no more files
   */
  protected def downloadNextFile(): Boolean

  /**
   * Processes the result of a downloaded file.
   *
   * @param filename the name of the file
   * @param downloadResult the result of the download attempt
   */
  protected def processDownloadedFile(filename: String, downloadResult: DownloadResult): Unit = {
    downloadResult match {
      case DownloadSuccess(lines, downloadMetrics, _) =>
        currentIterator = lines
        totalBytesDownloaded += downloadMetrics.byteCount
        filesProcessed += 1

      case DownloadFailure(error, durationMs) =>
        currentIterator = Iterator(error) // Return error line as record
        filesFailed += 1
        failedFiles += filename
        val durationStr = durationMs.map(ms => s" in ${ms}ms").getOrElse("")
        logError(s"[PARTITION-$partition]   ✗ Failed to download $filename$durationStr: $error")

    }
  }

  /**
   * Cleans up memory by draining the current iterator and clearing byte arrays.
   *
   * This is important to prevent memory leaks from unconsumed data when the reader is closed
   * before fully consuming the current file's data. By draining the iterator and clearing byte
   * arrays, we allow GC to reclaim memory that would otherwise be held indefinitely.
   */
  protected def cleanupMemory(): Unit = {
    // Clean up current iterator to release any unconsumed data
    if (currentIterator.hasNext) {

      // Drain iterator and clear byte arrays to release memory
      // Mutable loop counter required for tracking unconsumed iterator elements during cleanup
      // scalafix:off DisableSyntax.var
      var unconsumedCount = 0
      // scalafix:on DisableSyntax.var
      currentIterator.foreach { rawLine =>
        rawLine.logContent.foreach(arr => util.Arrays.fill(arr, 0.toByte))
        rawLine.rawFileBinary.foreach(arr => util.Arrays.fill(arr, 0.toByte))
        unconsumedCount += 1
      }

    }

    // Null assignment required to release GC-eligible references after cleanup
    // scalafix:off DisableSyntax.null
    currentRow = null
    currentIterator = null
    // scalafix:on DisableSyntax.null
    failedFiles.clear()
  }

  /**
   * Closes the reader
   *
   * Idempotent: the async reader closes itself proactively when all files are processed and
   * Spark also calls close() at task completion, so this method may be invoked more than once.
   */
  override def close(): Unit = {

    // Make close() idempotent - return early if already closed
    if (isClosed) {
      logDebug(s"[PARTITION-$partition] close() already called, skipping")
      // scalafix:off DisableSyntax.return
      return
      // scalafix:on DisableSyntax.return
    }
    isClosed = true

    if (filesFailed > 0) {
      logWarning(s"[PARTITION-$partition]   ✗ Failed files: ${failedFiles.mkString(", ")}")
    }

    try {
      closeClient()
      logDebug(s"[PARTITION-$partition]   Client closed successfully")
    } catch {
      case NonFatal(e) =>
        logWarning(s"[PARTITION-$partition]   Error closing client: ${e.getMessage}")
    }

    cleanupMemory()

    logDebug(s"[PARTITION-$partition] All resources released and nulled for GC")
  }

  /**
   * Abstract method to close the remote client and release any resources.
   *
   * Implementations should handle closing the client and any associated resources.
   * The framework will call this method when the reader is closed, ensuring proper cleanup.
   */
  protected def closeClient(): Unit

  /**
   * Reports custom metrics for this partition reader.
   *
   * @return array of custom task metrics including files, bytes, duration, and throughput
   */
  override def currentMetricsValues(): Array[CustomTaskMetric] = buildMetrics()

  /**
   * Returns the current row.
   */
  override def get(): InternalRow = currentRow
}
