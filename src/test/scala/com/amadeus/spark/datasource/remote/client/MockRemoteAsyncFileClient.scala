package com.amadeus.spark.datasource.remote.client

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions

import java.net.http.HttpClient
import java.sql.Timestamp
import java.time.LocalDateTime
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Mock implementation of RemoteFileClient for testing.
 * Returns predefined data without making actual HTTP calls.
 */
class MockRemoteAsyncFileClient extends AsyncRemoteFileClient {

  // Mutable state required for mock client options tracking in tests
  // scalafix:off DisableSyntax.var
  private[client] var options: RemoteFileDataSourceOptions = _
  // scalafix:on DisableSyntax.var

  override def init(opts: RemoteFileDataSourceOptions): Unit = {
    this.options = opts
  }

  // Predefined mock data
  private val mockFiles = mutable.LinkedHashMap(
    "file1.log" -> """{"timestamp":"2024-01-01T10:00:00Z","message":"Log entry 1"}""",
    "file2.log" -> """{"timestamp":"2024-01-01T10:01:00Z","message":"Log entry 2"}""",
    "file3.log" -> """{"timestamp":"2024-01-01T10:02:00Z","message":"Log entry 3"}"""
  )

  override def listLogFiles(): Seq[RemoteFile] = {
    mockFiles.keys.map(name => RemoteFile(name, Timestamp.valueOf(LocalDateTime.now()))).toSeq
  }

  override def downloadLogFileWithResult(filename: String): DownloadResult = {

    mockFiles.get(filename) match {
      case Some(content) =>
        val timestamp  = Timestamp.valueOf(LocalDateTime.now())
        val remoteFile = RemoteFile(filename, timestamp)
        val fileBytes  = content.getBytes("UTF-8")

        // Parse the file using ImpervaLogParser
        val parseResult = TestLogParser.parse(fileBytes)

        // Create single RawFileLine for the entire file
        val rawFileLine = RawFileLine(
          sourceFileMetadata = remoteFile,
          logMetadata = parseResult.logMetadata,
          logContent = parseResult.logContent,
          rawFileBinary = parseResult.rawFileBinary,
          downloadTimestamp = timestamp,
          error = parseResult.error
        )

        DownloadSuccess(Iterator.single(rawFileLine), DownloadMetrics(1, fileBytes.length))
      case None =>
        val timestamp  = new java.sql.Timestamp(System.currentTimeMillis())
        val remoteFile = RemoteFile(filename, timestamp)
        DownloadFailure(RawFileLine(remoteFile, downloadTimestamp = timestamp, error = Some(s"File not found: $filename")))
    }
  }

  override def close(): Unit = {
    // No resources to close in mock
  }

  override def shortName(): String = "mock-async"

  /**
   * Add a mock file for testing.
   */
  def addMockFile(filename: String, content: String): Unit = {
    val _ = mockFiles.put(filename, content)
  }

  /**
   * Clear all mock files.
   */
  def clearMockFiles(): Unit = {
    mockFiles.clear()
  }

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
   * @param httpClient       shared HttpClient with connection pooling
   */
  override def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit = {}

  /**
   * Asynchronously lists available log files from the remote endpoint.
   *
   * Uses the ExecutionContext provided via initAsync(). Clients should call
   * initAsync() before calling this method.
   *
   * @return Future containing the list of available log files
   */
  override def listLogFilesAsync(): Future[Seq[RemoteFile]] = throw new Exception("submitDownloadAsync is not implemented in MockRemoteAsyncFileClient")

  /**
   * Submits an asynchronous download request.
   *
   * Returns immediately with a Future that will complete when the download
   * finishes. Uses the ExecutionContext provided via initAsync().
   *
   * @param filename name of the file to download
   * @return Future[DownloadResult] that completes when download finishes
   */
  override def submitDownloadAsync(filename: String): Future[DownloadResult] = throw new Exception("submitDownloadAsync is not implemented in MockRemoteAsyncFileClient")
}
