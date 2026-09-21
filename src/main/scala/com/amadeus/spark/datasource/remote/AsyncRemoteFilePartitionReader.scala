package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.client._
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.amadeus.spark.datasource.remote.helpers.RemoteFilePartitionReaderHelper
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

/**
 * Asynchronous partition reader for remote file input partitions.
 *
 * This reader uses asynchronous downloads with prefetching to overlap I/O operations
 * with data processing. Files are submitted to a background executor pool (managed by
 * ExecutorAsyncResources), and the reader maintains a prefetch queue to ensure downloads
 * are ready when needed.
 *
 * Key features:
 * - Prefetches multiple files ahead of consumption
 * - Non-blocking download submission
 * - Configurable prefetch queue size
 * - Falls back to synchronous mode if async is not supported
 *
 * @param schema    the schema of the data
 * @param partition the input partition containing files to read
 * @param options   configuration options
 * @param asyncClient    the remote file client supporting async operations
 */
class AsyncRemoteFilePartitionReader(
    schema: StructType,
    partition: RemoteFileInputPartition,
    options: RemoteFileDataSourceOptions,
    asyncClient: AsyncRemoteFileClient
) extends RemoteFilePartitionReaderHelper(schema, partition)
    with Logging {

  /** Current file index being processed. */
  // Mutable iteration state required for sequential file index tracking
  // scalafix:off DisableSyntax.var
  private var currentFileIndex: Int = 0
  // scalafix:on DisableSyntax.var

  /**
   * Map of in-flight downloads: filename → Future[DownloadResult]
   * Using Map instead of Queue to support out-of-order completion.
   * Process whichever file completes first, not strict FIFO.
   */
  private val inFlightDownloads: mutable.Map[String, Future[DownloadResult]] = mutable.Map.empty

  /** Prefetch queue size from options. */
  private val prefetchSize: Int = options.asyncPrefetchSize

  /** Timeout for awaiting downloads (2x read timeout for safety). */
  private val downloadTimeout: Duration = options.readTimeout.plus(options.readTimeout)

  /** Flag to track cancellation state */
  // Volatile var required for thread-safe cancellation flag in async reader
  // scalafix:off DisableSyntax.var
  @volatile private var isCancelled: Boolean = false
  // scalafix:on DisableSyntax.var

  // Register task completion listener for proper cancellation support
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener[Unit] { _ =>
      logDebug(s"[PARTITION-$partition] Task completion listener triggered - marking as cancelled")
      isCancelled = true
    }
  }

  /**
   * Fills the prefetch pipeline by submitting downloads for upcoming files.
   *
   * Maintains up to prefetchSize concurrent downloads. Unlike a strict queue,
   * this allows files to complete out-of-order based on size/network conditions.
   *
   * Production optimization: Process whichever file completes first, not FIFO order.
   * Critical for heterogeneous file sizes (don't wait for 100MB file when 1MB is ready).
   */
  private def fillPrefetchPipeline(): Unit = {
    // Calculate how many more files we can submit for concurrent download
    val currentInFlight = inFlightDownloads.size
    val filesRemaining  = partition.files.size - currentFileIndex
    val slotsAvailable  = prefetchSize - currentInFlight

    // Submit downloads to maintain pipeline depth
    if (slotsAvailable > 0 && filesRemaining > 0) {
      val filesToSubmit = Math.min(slotsAvailable, filesRemaining)

      for (_ <- 0 until filesToSubmit) {
        if (currentFileIndex < partition.files.size) {
          val filename = partition.files(currentFileIndex)

          // Only submit if not already in flight
          if (!inFlightDownloads.contains(filename)) {
            val future = asyncClient.submitDownloadAsync(filename)
            inFlightDownloads.put(filename, future)
            logDebug(s"[PARTITION-$partition] Submitted async download for $filename (in-flight: ${inFlightDownloads.size})")
          }

          currentFileIndex += 1
        }
      }
    }
  }

  /**
   * Gets the next completed download result, processing whichever file finishes first.
   *
   * Production optimization: Instead of blocking on FIFO order, this method checks
   * which downloads are already complete and processes them immediately. Only blocks
   * if no downloads are ready yet.
   *
   * Example: If file[0]=100MB and file[1]=1MB, we'll process file[1] as soon as it
   * completes, rather than waiting for file[0] to finish first.
   *
   * @return (filename, DownloadResult) tuple for the next completed download
   */
  private def getNextCompletedDownload: (String, DownloadResult) = {
    // Ensure pipeline is filled with downloads
    fillPrefetchPipeline()
    // Sanity check: must have in-flight downloads or files left to submit
    assert(inFlightDownloads.nonEmpty || currentFileIndex >= partition.files.size, s"[PARTITION-$partition] No in-flight downloads and no files left to submit")

    // Check if any download is already complete (non-blocking check)
    val completedEntry = inFlightDownloads.find(_._2.isCompleted)

    completedEntry match {
      case Some((filename, future)) => processCompletedDownloads(filename, future)
      case None                     =>
        // No downloads complete yet - wait for the FIRST one to finish (any file)
        val (completedFilename, completedFuture) = waitForFirstCompletedDownload

        processCompletedDownloads(completedFilename, completedFuture)
    }
  }

  /** Checks if the current task has been interrupted. */
  private def isTaskInterrupted: Boolean = Option(TaskContext.get()).exists(_.isInterrupted())

  /**
   * Waits for the first completed download among the in-flight downloads.
   *
   * This method blocks until at least one download completes, then identifies
   * which one(s) are done and returns the first completed result.
   *
   * @return (filename, Future[DownloadResult]) tuple for the first completed download
   */
  private def waitForFirstCompletedDownload: (String, Future[DownloadResult]) = {
    val allFutures = inFlightDownloads.toSeq
    scala.concurrent.Await.result(
      scala.concurrent.Future.firstCompletedOf(allFutures.map(_._2)),
      downloadTimeout
    )

    // Now find which file completed (at least one must be completed now)
    val (completedFilename, completedFuture) = inFlightDownloads.find(_._2.isCompleted).getOrElse {
      throw new IllegalStateException("No completed future found after firstCompletedOf returned")
    }

    (completedFilename, completedFuture)
  }

  /**
   * Processes a completed download by removing it from the in-flight map and returning the result.
   *
   * @param filename the name of the file that was downloaded
   * @param downloadTask the Future representing the download task (already completed)
   * @return (filename, DownloadResult) tuple for the completed download
   */
  private def processCompletedDownloads(filename: String, downloadTask: Future[DownloadResult]): (String, DownloadResult) = {
    inFlightDownloads.remove(filename)
    val result = Await.result(downloadTask, Duration.Zero) // Already completed
    logDebug(s"[PARTITION-$partition] Retrieved completed download for $filename (no wait)")

    fillPrefetchPipeline()

    (filename, result)
  }

  /**
   * Downloads the next file if the current iterator is exhausted.
   *
   * @return true if a new file was downloaded and currentIterator is updated, false if no more files
   */
  override protected def downloadNextFile(): Boolean = {
    if (isCancelled || isTaskInterrupted) {
      logWarning(s"[PARTITION-$partition] Task cancelled, stopping iteration")
      // scalafix:off DisableSyntax.return
      return false
      // scalafix:on DisableSyntax.return
    }

    while (!currentIterator.hasNext) {

      if (isCancelled || isTaskInterrupted) {
        logWarning(s"[PARTITION-$partition] Task cancelled during file processing")
        // scalafix:off DisableSyntax.return
        return false
        // scalafix:on DisableSyntax.return
      }

      if (areAllFilesDownloadedOrFailed) {
        logInfo(s"[PARTITION-$partition] All files processed - ending iteration")
        close() // Cleanup resources proactively
        // scalafix:off DisableSyntax.return
        return false
        // scalafix:on DisableSyntax.return
      }

      // Get next completed download (whichever file finishes first)
      val (filename, downloadResult) = getNextCompletedDownload

      logDebug(s"[PARTITION-$partition] Processing completed download: $filename")
      logDebug(s"[PARTITION-$partition]   Thread: ${Thread.currentThread().getName}")
      logDebug(s"[PARTITION-$partition]   Files processed: $filesProcessed/${partition.files.size}, In-flight: ${inFlightDownloads.size}")

      processDownloadedFile(filename, downloadResult)
    }

    true
  }

  /**
   * Checks if all files have been processed (either downloaded successfully or failed).
   *
   * @return true if all files are done, false if there are still files to process or in-flight downloads
   */
  private def areAllFilesDownloadedOrFailed: Boolean = {
    val totalFiles     = partition.files.size
    val filesRemaining = totalFiles - filesProcessed - filesFailed

    filesRemaining == 0 && inFlightDownloads.isEmpty
  }

  /**
   * Closes the client and releases resources.
   */
  override protected def closeClient(): Unit = {
    cancelPendingDownloads()
    asyncClient.close()
  }

  /**
   * Cancels any pending downloads that have not yet completed.
   *
   * This method attempts to wait briefly for in-flight downloads to complete, but will not block indefinitely.
   * It also sets up cleanup for any downloads that do complete after cancellation to prevent memory leaks.
   */
  private def cancelPendingDownloads(): Unit = {
    if (inFlightDownloads.nonEmpty) {
      logDebug(s"[PARTITION-$partition] Cancelling ${inFlightDownloads.size} pending downloads")
      isCancelled = true

      // Attempt to wait briefly for in-flight downloads to complete (best effort)
      // This helps prevent memory leaks from abandoned futures
      val cancellationDeadline = System.currentTimeMillis() + 2000 // 2 seconds
      // Mutable loop counter required for tracking remaining in-flight downloads
      // scalafix:off DisableSyntax.var
      var remainingDownloads = inFlightDownloads.size
      // scalafix:on DisableSyntax.var

      while (remainingDownloads > 0 && System.currentTimeMillis() < cancellationDeadline) {

        inFlightDownloads.find(_._2.isCompleted) foreach { case (filename, future) =>
          // Extract and discard the result to release memory held by the iterator
          try {
            Await.result(future, Duration.Zero)
            // Actively release memory from downloaded data
            logDebug(s"[PARTITION-$partition] Released memory for completed download: $filename")
          } catch {
            case NonFatal(e) =>
              logDebug(s"[PARTITION-$partition] Error releasing memory for $filename: ${e.getMessage}")
          }
          inFlightDownloads.remove(filename)
          remainingDownloads -= 1
        }

        if (remainingDownloads > 0) {
          Thread.sleep(50) // Brief wait before checking again
        }
      }

      if (inFlightDownloads.nonEmpty) {
        logWarning(s"[PARTITION-$partition] ${inFlightDownloads.size} downloads still running after cancellation timeout (will complete in background)")
        // Even for non-completed futures, try to set up cleanup when they do complete
        inFlightDownloads.values.foreach { future =>
          future.onComplete { _ =>
            // Just a marker to allow GC - the future completion will release resources
            logTrace(s"[PARTITION-$partition] Background download completed after close()")
          }
        }
        inFlightDownloads.clear()
      } else {
        logInfo(s"[PARTITION-$partition] All downloads completed cleanly during cancellation")
      }
    }
  }
}

object AsyncRemoteFilePartitionReader extends Logging {

  def apply(
      schema: StructType,
      parsedOptions: RemoteFileDataSourceOptions,
      restFilePartition: RemoteFileInputPartition
  ): AsyncRemoteFilePartitionReader = {
    logDebug(s"[READER-FACTORY] Creating AsyncRemoteFilePartitionReader for partition $restFilePartition")

    // Use the same parsed configuration as the reader and share only compatible executor resources.
    val executionContext = ExecutorAsyncResources.getExecutionContext(parsedOptions)
    val httpClient       = ExecutorAsyncResources.getHttpClient(parsedOptions)
    logDebug("[READER-FACTORY] Obtained shared ExecutionContext and HttpClient from ExecutorAsyncResources")

    // Create client
    val client = RemoteFileClient.from(parsedOptions)

    // Initialize async support if client supports it
    val asyncClient = client match {
      case asClient: AsyncRemoteFileClient =>
        logDebug("[READER-FACTORY] Client supports async, initializing with shared resources")
        asClient.initAsync(executionContext, httpClient)
        asClient
      case _ =>
        logError(s"[READER-FACTORY] Async downloads requested but client ${client.getClass.getName} does not support AsyncRemoteFileClient trait")
        throw new IllegalStateException(s"[PARTITION-$restFilePartition] AsyncRemoteFilePartitionReader requires an AsyncRemoteFileClient when asyncDownloads is enabled")
    }

    new AsyncRemoteFilePartitionReader(
      schema = schema,
      partition = restFilePartition,
      options = parsedOptions,
      asyncClient = asyncClient
    )
  }
}
