package com.amadeus.spark.datasource.remote.client

import java.net.http.HttpClient
import scala.concurrent.{ExecutionContext, Future}

/**
 * Optional trait for remote file clients that support asynchronous operations.
 *
 * Clients implementing this trait receive executor-scoped resources from the framework:
 * - ExecutionContext: Shared thread pool for async operations
 * - HttpClient: Shared HTTP client with connection pooling
 *
 * The framework handles lifecycle management - clients should NOT:
 * - Create their own thread pools or ExecutorService
 * - Create their own HttpClient instances
 * - Shut down these resources in close() method
 *
 * **Why Shared HttpClient?**
 * - HttpClient maintains an internal connection pool
 * - Sharing enables connection reuse across all tasks on the executor
 * - Dramatically reduces TCP overhead and memory consumption
 * - Critical for production performance at scale
 *
 * **Lifecycle:**
 * 1. Client is instantiated (per partition reader)
 * 2. Framework calls initAsync() with shared ExecutionContext and HttpClient
 * 3. Client uses these resources for async operations
 * 4. Client's close() method clears references but does NOT shut down resources
 *
 * Clients that don't implement this trait will fall back to synchronous behavior.
 */
trait AsyncRemoteFileClient extends RemoteFileClient {

  /**
   * Initializes async support with framework-provided resources.
   *
   * Called by the framework when async downloads are enabled. Both resources
   * are executor-scoped (shared across all tasks) and managed by ExecutorAsyncResources.
   *
   * Implementations should:
   * - Store the ExecutionContext and HttpClient for use in async operations
   * - Use the provided HttpClient (don't create new instances)
   * - Initialize any async backends (e.g., HttpClientFutureBackend) using these resources
   * - NOT shut down these resources in close() method
   *
   * @param executionContext shared ExecutionContext for async operations
   * @param httpClient shared HttpClient with connection pooling
   */
  def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit

  /**
   * Asynchronously lists available log files from the remote endpoint.
   *
   * Uses the ExecutionContext provided via initAsync(). Clients should call
   * initAsync() before calling this method.
   *
   * @return Future containing the list of available log files
   */
  def listLogFilesAsync(): Future[Seq[RemoteFile]]

  /**
   * Submits an asynchronous download request.
   *
   * Returns immediately with a Future that will complete when the download
   * finishes. Uses the ExecutionContext provided via initAsync().
   *
   * @param filename name of the file to download
   * @return Future[DownloadResult] that completes when download finishes
   */
  def submitDownloadAsync(filename: String): Future[DownloadResult]
}
