package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.client.{AsyncRemoteFileClient, RemoteFile}
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.internal.Logging

import java.net.http.HttpClient
import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Helper trait for managing asynchronous file discovery in WorkloadRefresher.
 *
 * This trait encapsulates the logic for initializing the async client, refreshing the file list asynchronously,
 * and waiting for async operations to complete when necessary.
 */
trait WorkloadRefresherAsyncHelper extends Logging {

  /** Track pending async operations */
  @volatile private var pendingFuture: Option[Future[Unit]] = None

  /** ExecutionContext for async operations */
  private implicit val executionContext: ExecutionContext = ExecutionContext.global

  /** Accessor for discovered files, to be implemented by WorkloadRefresher. */
  protected def discoveredFiles: Seq[RemoteFile]

  /**
   * Initializes the asynchronous client on the driver for latestOffset operations.
   *
   * @param asyncClient the asynchronous remote file client to initialize
   */
  protected def initAsyncClient(config: RemoteFileDataSourceOptions, asyncClient: AsyncRemoteFileClient): Unit = {
    logDebug("Initializing AsyncRemoteFileClient on driver for latestOffset operations")

    // Create a shared HttpClient for driver-side operations
    val httpClient = HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofMillis(config.connectionTimeout.toMillis))
      .build()

    asyncClient.initAsync(executionContext, httpClient)
    logDebug("AsyncRemoteFileClient initialized successfully on driver")
  }

  /**
   * Fetches files from the remote server using the asynchronous client.
   *
   * @param asyncClient the asynchronous remote file client
   * @return Sequence of RemoteFile objects fetched asynchronously
   */
  protected def fetchFilesAsyncFromServer(asyncClient: AsyncRemoteFileClient): Seq[RemoteFile] = {
    logDebug(s"  fetchFilesFromServer: Using async mode with AsyncRemoteFileClient")

    try {
      val future = asyncClient.listLogFilesAsync()
      val files  = Await.result(future, 30.seconds)
      logDebug(s"  fetchFilesFromServer: Async fetched ${files.size} files from API")
      files
    } catch {
      case e: Exception =>
        logWarning(s"  fetchFilesFromServer: Error fetching files asynchronously: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Refreshes the list of available files using the asynchronous client.
   *
   * The async call updates discoveredFiles in the background when it completes.
   * If discoveredFiles is empty, waits for the async call to complete before proceeding.
   *
   * @param asyncClient the asynchronous remote file client
   */
  protected def refreshAsyncFileList(asyncClient: AsyncRemoteFileClient): Unit = {

    logDebug(s"  refreshFileList: Using async mode with AsyncRemoteFileClient")

    val future = asyncClient
      .listLogFilesAsync()
      .map { indexFiles =>
        logDebug(s"  refreshFileList: Async discovered ${indexFiles.size} files from API")
        mergeFiles(indexFiles)
      }
      .recover { case e: Exception =>
        logError(s"  refreshFileList: Async call failed: ${e.getMessage}", e)
      }

    pendingFuture = Some(future)

    // If discoveredFiles is empty, wait for the future to complete
    // so we have files available for the first workload refresh
    if (discoveredFiles.isEmpty) {
      waitForAsyncRefreshCompletion(future)
    }
  }

  /**
   * Merges newly discovered files into the existing list of discovered files.
   *
   * @param indexFiles sequence of RemoteFile objects fetched from the API
   */
  protected def mergeFiles(indexFiles: Seq[RemoteFile]): Unit

  /**
   * Waits for the asynchronous refresh operation to complete and updates the pendingFuture state.
   *
   * @param future the Future representing the asynchronous refresh operation
   */
  private def waitForAsyncRefreshCompletion(future: Future[Unit]): Unit = {
    try {
      logDebug("  refreshFileList: discoveredFiles is empty, waiting for async call to complete...")
      Await.result(future, 30.seconds)
      pendingFuture = None
      logDebug("  refreshFileList: Initial async call completed")
    } catch {
      case e: Exception =>
        logWarning(s"  refreshFileList: Error waiting for initial async call: ${e.getMessage}")
        pendingFuture = None
    }
  }

  /**
   * Waits for any pending asynchronous operation to complete.
   */
  protected def waitForPendingAsyncOperation(): Unit = {
    pendingFuture.foreach { future =>
      try {
        logDebug("  Waiting for pending async operation to complete...")
        Await.result(future, 5.seconds)
        logDebug("  Async operation completed")
      } catch {
        case e: Exception =>
          logWarning(s"  Error waiting for async operation: ${e.getMessage}")
      }
    }
  }

}
