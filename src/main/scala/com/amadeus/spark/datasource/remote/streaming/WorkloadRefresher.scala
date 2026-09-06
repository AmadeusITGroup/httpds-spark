package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.client.{AsyncRemoteFileClient, RemoteFile, RemoteFileClient}
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.amadeus.spark.datasource.remote.streaming.WorkloadRefresher.ordered
import org.apache.spark.internal.Logging

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration

/**
 * Workload refresher for polling and discovering new log files from Rest endpoint.
 *
 * @param config configuration options
 */
class WorkloadRefresher(config: RemoteFileDataSourceOptions) extends WorkloadRefresherAsyncHelper with Logging {

  /** The remote client used to fetch log files. */
  private lazy val client: RemoteFileClient = {
    val remoteFileClient = RemoteFileClient.from(config)

    remoteFileClient match {
      case asyncClient: AsyncRemoteFileClient if config.asyncListFiles => initAsyncClient(config, asyncClient)
      case _ =>
        logDebug("Client does not support async or asyncListFiles is disabled")
    }

    remoteFileClient
  }

  /** Timestamp of the last poll to Incapsula API. */
  // Mutable state required for tracking poll interval timing
  // scalafix:off DisableSyntax.var
  private var lastPollTime: Long = 0L
  // scalafix:on DisableSyntax.var

  /** List of discovered log files. Thread-safe for async updates. */
  private val discoveredFilesRef: AtomicReference[Seq[RemoteFile]] = new AtomicReference(Seq.empty)

  /** Accessor for discovered files with thread-safe read */
  override protected def discoveredFiles: Seq[RemoteFile] = discoveredFilesRef.get()

  /** Thread-safe update for discovered files */
  private def discoveredFiles_=(newFiles: Seq[RemoteFile]): Unit = discoveredFilesRef.set(newFiles)

  /**
   * Returns the number of discovered files.
   *
   * @return number of discovered files
   */
  def nbDiscoveredFiles: Int = discoveredFiles.size

  /**
   * Refreshes the workload by polling for new files and determining which files to process.
   *
   * @param previouslyProcessedFiles set of file names that have already been processed
   * @param maxFiles maximum number of files to process in this trigger
   * @return Workload containing files to process and full file list
   */
  def refreshWorkload(previouslyProcessedFiles: Set[String], maxFiles: Int): Workload = {
    logDebug(s"  Max files per trigger: $maxFiles")

    refreshFileList()

    val lastIndexFiles = discoveredFiles
    val filesNotProcessed = lastIndexFiles
      .filterNot(f => previouslyProcessedFiles.contains(f.name))
      .sortBy(_.fetchedAt)

    logDebug(s"  Total discovered files: ${discoveredFiles.size}")
    logDebug(s"  Available files: ${filesNotProcessed.size}")

    // Intentionally retain processed names only while listed; forgotten files may be processed again if they reappear.
    val filesToProcess            = filesNotProcessed.take(maxFiles).map(_.name)
    val filesStillListedProcessed = lastIndexFiles.map(_.name).filter(f => previouslyProcessedFiles.contains(f))
    val fullFileList              = filesToProcess ++ filesStillListedProcessed

    Workload(filesToProcess, fullFileList.toSet)
  }

  /**
   * Fetches files from the remote server, handling both async and sync clients.
   *
   * @return Sequence of RemoteFile objects
   */
  private def fetchFilesFromServer(): Seq[RemoteFile] = {
    client match {
      case asyncClient: AsyncRemoteFileClient if config.asyncListFiles => fetchFilesAsyncFromServer(asyncClient)
      case _ =>
        val files = client.listLogFiles()
        logDebug(s"  fetchFilesFromServer - synchronous:  Fetched ${files.size} files from API")
        files
    }
  }

  /**
   * Refreshes the list of available files from client.
   *
   * If asyncListFiles is enabled, the HTTP call is made asynchronously and discoveredFiles
   * is updated in the background. Otherwise, uses blocking synchronous call.
   */
  private def refreshFileList(): Unit = {
    val now = System.currentTimeMillis()
    if (WorkloadRefresher.hasToSkipPull(lastPollTime, config.pollingInterval)) {
      logDebug(s"  refreshFileList: Skipping poll - last poll was ${now - lastPollTime}ms ago (interval: ${config.pollingInterval.toMillis}ms)")
      // scalafix:off DisableSyntax.return
      return
      // scalafix:on DisableSyntax.return
    }

    logDebug(s"  refreshFileList: Polling for new files (last poll: ${now - lastPollTime}ms ago)")

    client match {
      case asyncClient: AsyncRemoteFileClient if config.asyncListFiles => refreshAsyncFileList(asyncClient)
      case _ =>
        val indexFiles = client.listLogFiles()
        logDebug(s"  refreshFileList - synchronous : Discovered ${indexFiles.size} files from API")
        mergeFiles(indexFiles)
    }

    lastPollTime = now
  }

  /**
   * Gets all currently available files from the remote server.
   * This is used for "latest" mode to skip existing files at stream start.
   *
   * @return Set of all currently available file names
   */
  def getAllCurrentFiles: Set[String] = {
    logDebug("  getAllCurrentFiles: Fetching all currently available files for latest mode")
    val files = fetchFilesFromServer()
    files.map(_.name).toSet
  }

  /**
   * Discovers ALL available files from the remote source without applying any limits.
   *
   * This is used for Trigger.AvailableNow() mode to cache the complete file list
   * at query start time (snapshot isolation). Rate limiting is applied by the caller.
   *
   * Unlike refreshWorkload(), this method:
   * - Does NOT apply maxFiles limit
   * - Does NOT filter by processed files
   * - Returns ALL files for snapshot caching
   *
   * @return Set of all available file names
   */
  def discoverAllFiles(): Set[String] = {
    logInfo("discoverAllFiles: Discovering all available files for AvailableNow snapshot")
    val files     = fetchFilesFromServer()
    val fileNames = files.map(_.name).toSet
    logInfo(s"discoverAllFiles: Discovered ${fileNames.size} total files for snapshot")
    logDebug(s"discoverAllFiles: Files: ${fileNames.mkString(", ")}")
    fileNames
  }

  /**
   * Closes the client and waits for pending async operations.
   */
  def stop(): Unit = {
    logInfo("Closing client...")

    waitForPendingAsyncOperation()

    try {
      client.close()
      logDebug("  Client closed successfully")
    } catch {
      case e: Exception =>
        logWarning(s"  Error closing client: ${e.getMessage}")
    }
  }

  /**
   * Merges newly discovered files into the existing list of discovered files.
   *
   * @param indexFiles sequence of RemoteFile objects fetched from the API
   */
  override protected def mergeFiles(indexFiles: Seq[RemoteFile]): Unit = {
    discoveredFiles = WorkloadRefresher.mergeFiles(discoveredFiles, indexFiles)
  }
}

object WorkloadRefresher {

  /**
   * Merges old and fresh file lists, keeping the earliest fetchedAt for duplicates.
   *
   * @param oldFiles previously known files
   * @param freshFiles newly fetched files
   * @return merged list of [[RemoteFile]]
   */
  private[streaming] def mergeFiles(oldFiles: Seq[RemoteFile], freshFiles: Seq[RemoteFile]): Seq[RemoteFile] = {
    val freshFileNames = freshFiles.map(_.name).toSet
    val commonFiles    = oldFiles.filter(of => freshFileNames.contains(of.name))
    val combinedFiles  = commonFiles ++ freshFiles

    combinedFiles
      .groupBy(_.name)
      .map { case (_, files) =>
        files.minBy(_.fetchedAt)
      }
      .toSeq
      .sortBy(f => (f.fetchedAt, f.name))
  }

  /**
   * Determines if the pull should be skipped based on the last poll time and polling interval.
   *
   * @param lastPollTime timestamp of the last poll
   * @param pollingInterval configured polling interval
   * @return true if the pull should be skipped, false otherwise
   */
  private def hasToSkipPull(lastPollTime: Long, pollingInterval: Duration): Boolean = {
    val now = System.currentTimeMillis()
    now - lastPollTime < pollingInterval.toMillis
  }

  /** Implicit ordering for Timestamp to sort by fetchedAt. */
  implicit def ordered: Ordering[Timestamp] = (x: Timestamp, y: Timestamp) => x compareTo y

}
