package com.amadeus.spark.datasource.remote.conf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.URI
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.matching.Regex

/**
 * Configuration options for the Remote File Data Source.
 *
 * @param remoteClient       The short name or class name of the remote file client implementation
 * @param apiKey             Optional API key for authentication
 * @param apiSecret          Optional API secret for authentication
 * @param numPartitions      Number of partitions for parallelism
 * @param connectionTimeout  HTTP connection timeout duration
 * @param readTimeout        HTTP read timeout duration
 * @param maxRetries         Maximum number of retries for failed requests
 * @param retryDelay         Delay between retries
 * @param enableSsl          Whether to enable SSL/TLS
 * @param trustStorePath     Optional path to the trust store for SSL
 * @param trustStorePassword Optional password for the trust store
 * @param serverUri          URI of the remote file server (can be set via "uri" or "path" option)
 * @param sessionId          Optional session ID for isolated scenario state (useful for testing)
 * @param pollingInterval    Interval between polling for new files in streaming mode, it limits only the frequency of index API calls.
 * @param maxFilesPerTrigger Maximum number of files to process per trigger in streaming mode
 * @param asyncListFiles     If true, listLogFiles calls are asynchronous and discoveredFiles is updated in the background
 * @param asyncDownloads     If true, enables asynchronous file downloads with prefetching
 * @param asyncPrefetchSize  Number of files to prefetch when async downloads are enabled
 * @param asyncDownloadThreads Number of threads in the download executor pool
 * @param startOffset        Mode for initial offset: "earliest" (start from beginning), "latest" (start from current position), or "file:<filename>" (start from specified filename)
 */
case class RemoteFileDataSourceOptions(
    remoteClient: String,
    apiKey: Option[String] = None,
    apiSecret: Option[String] = None,
    sessionId: Option[String] = None,
    numPartitions: Int = RemoteFileDataSourceOptions.DEFAULT_NUM_PARTITIONS,
    connectionTimeout: Duration = RemoteFileDataSourceOptions.DEFAULT_CONNECTION_TIMEOUT,
    readTimeout: Duration = RemoteFileDataSourceOptions.DEFAULT_READ_TIMEOUT,
    maxRetries: Int = RemoteFileDataSourceOptions.DEFAULT_MAX_RETRIES,
    retryDelay: Duration = RemoteFileDataSourceOptions.DEFAULT_RETRY_DELAY,
    enableSsl: Boolean = false,
    trustStorePath: Option[String] = None,
    trustStorePassword: Option[String] = None,
    serverUri: String = RemoteFileDataSourceOptions.DEFAULT_LOG_SERVER_URI,
    pollingInterval: Duration = RemoteFileDataSourceOptions.DEFAULT_POLLING_INTERVAL,
    maxFilesPerTrigger: Int = RemoteFileDataSourceOptions.DEFAULT_MAX_FILES_PER_TRIGGER,
    asyncListFiles: Boolean = RemoteFileDataSourceOptions.DEFAULT_ASYNC_LIST_FILES,
    asyncDownloads: Boolean = RemoteFileDataSourceOptions.DEFAULT_ASYNC_DOWNLOADS,
    asyncPrefetchSize: Int = RemoteFileDataSourceOptions.DEFAULT_ASYNC_PREFETCH_SIZE,
    asyncDownloadThreads: Int = RemoteFileDataSourceOptions.DEFAULT_ASYNC_DOWNLOAD_THREADS,
    startOffset: String = RemoteFileDataSourceOptions.DEFAULT_START_OFFSET,
    allOptions: CaseInsensitiveStringMap
) {

  /**
   * Validates that all required options are present and valid.
   *
   * @throws IllegalArgumentException if validation fails
   */
  def validate(): Unit = {
    require(numPartitions > 0, s"'${RemoteFileDataSourceOptions.PARTITIONS}' must be positive")
    require(maxRetries >= 0, s"'${RemoteFileDataSourceOptions.MAX_RETRIES}' must be non-negative")
    require(maxFilesPerTrigger > 0, s"'${RemoteFileDataSourceOptions.MAX_FILES}' must be positive")
    require(serverUri.nonEmpty, s"'${RemoteFileDataSourceOptions.URI}' must not be empty")
    require(remoteClient.nonEmpty, s"'${RemoteFileDataSourceOptions.REMOTE_CLIENT}' must not be empty")

    validateUriFormat()
  }

  /**
   * Validates that the server URI is a well-formed URI.
   *
   * @throws IllegalArgumentException if the URI is invalid
   */
  private def validateUriFormat(): Unit = {
    try {
      new URI(serverUri)
    } catch {
      case e: Exception => throw new IllegalArgumentException(s"'${RemoteFileDataSourceOptions.URI}' is not a valid URI: $serverUri", e)
    }
  }

  /**
   * Converts this configuration to a Map for serialization.
   */
  def toMap: Map[String, String] = {
    val builder = Map.newBuilder[String, String]

    apiKey.foreach(builder += RemoteFileDataSourceOptions.API_KEY                          -> _)
    apiSecret.foreach(builder += RemoteFileDataSourceOptions.API_SECRET                    -> _)
    builder += RemoteFileDataSourceOptions.PARTITIONS                                      -> numPartitions.toString
    builder += RemoteFileDataSourceOptions.CONNECT_TIMEOUT                                 -> connectionTimeout.toString
    builder += RemoteFileDataSourceOptions.READ_TIMEOUT                                    -> readTimeout.toString
    builder += RemoteFileDataSourceOptions.MAX_RETRIES                                     -> maxRetries.toString
    builder += RemoteFileDataSourceOptions.RETRY_DELAY                                     -> retryDelay.toString
    builder += RemoteFileDataSourceOptions.ENABLE_SSL                                      -> enableSsl.toString
    trustStorePath.foreach(builder += RemoteFileDataSourceOptions.TRUST_STORE_PATH         -> _)
    trustStorePassword.foreach(builder += RemoteFileDataSourceOptions.TRUST_STORE_PASSWORD -> _)
    builder += RemoteFileDataSourceOptions.URI                                             -> serverUri
    builder += RemoteFileDataSourceOptions.POLL_INTERVAL                                   -> pollingInterval.toString
    builder += RemoteFileDataSourceOptions.MAX_FILES                                       -> maxFilesPerTrigger.toString
    builder += RemoteFileDataSourceOptions.REMOTE_CLIENT                                   -> remoteClient.toString
    builder += RemoteFileDataSourceOptions.ASYNC_LIST_FILES                                -> asyncListFiles.toString
    builder += RemoteFileDataSourceOptions.ASYNC_DOWNLOADS                                 -> asyncDownloads.toString
    builder += RemoteFileDataSourceOptions.ASYNC_PREFETCH_SIZE                             -> asyncPrefetchSize.toString
    builder += RemoteFileDataSourceOptions.ASYNC_DOWNLOAD_THREADS                          -> asyncDownloadThreads.toString
    builder += RemoteFileDataSourceOptions.START_OFFSET                                    -> startOffset

    builder.result()
  }

  /**
   * Returns a human-readable summary of this configuration for logging.
   * Sensitive values (API keys, passwords) are masked.
   */
  def toLogString: String = {
    s"""Remote File DataSource Configuration:
       |  restClientClass = $remoteClient
       |  uri             = $serverUri
       |  pollInterval    = ${pollingInterval.toString}
       |  maxFiles        = $maxFilesPerTrigger
       |  partitions      = $numPartitions
       |  connectTimeout  = ${connectionTimeout.toString}
       |  readTimeout     = ${readTimeout.toString}
       |  maxRetries      = $maxRetries
       |  retryDelay      = ${retryDelay.toString}
       |  enableSsl       = $enableSsl
       |  apiKey          = ${apiKey.map(_ => "***").getOrElse("<not set>")}
       |  startOffset     = $startOffset
       """.stripMargin
  }
}

/**
 * Companion object with factory methods and constants.
 */
object RemoteFileDataSourceOptions extends Logging {

  // ============================================================
  // Configuration Keys
  // ============================================================

  /** Server URI (option name) */
  private val URI: String = "uri"

  /** Path option — alias for URI, compatible with Spark's DataStreamReader.load(path) */
  private val PATH: String = "path"

  /** remote client class */
  private val REMOTE_CLIENT: String = "remoteClient"

  // Streaming options
  private val POLL_INTERVAL: String    = "pollInterval"
  private val MAX_FILES: String        = "maxFilesPerTrigger"
  private val ASYNC_LIST_FILES: String = "asyncListFiles"

  // Async download options
  private val ASYNC_DOWNLOADS: String        = "asyncDownloads"
  private val ASYNC_PREFETCH_SIZE: String    = "asyncPrefetchSize"
  private val ASYNC_DOWNLOAD_THREADS: String = "asyncDownloadThreads"

  // Start offset mode
  private val START_OFFSET: String = "startOffset"

  // Parallelism
  private val PARTITIONS: String = "partitions"

  // HTTP client options
  private val CONNECT_TIMEOUT: String = "connectTimeout"
  private val READ_TIMEOUT: String    = "readTimeout"
  private val MAX_RETRIES: String     = "maxRetries"
  private val RETRY_DELAY: String     = "retryDelay"

  // Authentication (Phase 1)
  private val API_KEY: String    = "apiKey"
  private val API_SECRET: String = "apiSecret"

  // Session isolation (for testing)
  private val SESSION_ID: String = "sessionId"

  // SSL/TLS (Phase 1)
  private val ENABLE_SSL: String           = "enableSsl"
  private val TRUST_STORE_PATH: String     = "trustStorePath"
  private val TRUST_STORE_PASSWORD: String = "trustStorePassword"

  /** Default server URI */
  private val DEFAULT_LOG_SERVER_URI: String = "http://localhost:5000"

  /** Default number of partitions */
  private val DEFAULT_NUM_PARTITIONS: Int = 4

  /** Default timeouts and retry settings */
  private val DEFAULT_CONNECTION_TIMEOUT: Duration = Duration(30, SECONDS)

  /** Default read timeout */
  private val DEFAULT_READ_TIMEOUT: Duration = Duration(60, SECONDS)

  /** Default maximum number of retries */
  private val DEFAULT_MAX_RETRIES: Int = 3

  /** Default delay between retries */
  private val DEFAULT_RETRY_DELAY: Duration = Duration(1, SECONDS)

  /** Default polling interval */
  private val DEFAULT_POLLING_INTERVAL: Duration = Duration(2, SECONDS)

  /** Default maximum files per trigger */
  private val DEFAULT_MAX_FILES_PER_TRIGGER: Int = 100

  /** Default async list files setting */
  private val DEFAULT_ASYNC_LIST_FILES: Boolean = false

  /** Default async downloads setting */
  private val DEFAULT_ASYNC_DOWNLOADS: Boolean = false

  /** Default async prefetch size */
  private val DEFAULT_ASYNC_PREFETCH_SIZE: Int = 20

  /** Default async download threads */
  private val DEFAULT_ASYNC_DOWNLOAD_THREADS: Int = 4

  /** Default start offset mode */
  private val DEFAULT_START_OFFSET: String = "earliest"

  val filenameOffsetPattern: Regex = """^file:(.+)$""".r

  /**
   * Creates an ImpervaDataSourceOptions from a Java Map.
   *
   * @param options the options map
   * @return a new RemoteFileDataSourceOptions instance
   */
  def fromMap(options: java.util.Map[String, String]): RemoteFileDataSourceOptions = {
    fromMap(new CaseInsensitiveStringMap(options))
  }

  /**
   * Creates a [[RemoteFileDataSourceOptions]] from a CaseInsensitiveStringMap.
   *
   * Supports duration parsing for time-based options:
   *  - "10s" → 10 seconds
   *  - "500ms" → 500 milliseconds
   *  - "5m" → 5 minutes
   *  - "10000" → 10000 milliseconds (plain number)
   *
   * @param dataSourceOptions the options map
   * @return a new RemoteFileDataSourceOptions instance
   */
  def fromMap(dataSourceOptions: CaseInsensitiveStringMap): RemoteFileDataSourceOptions = {
    // Helper to parse duration values
    def getDuration(key: String, default: Duration): Duration = {
      Option(dataSourceOptions.get(key))
        .map(Duration(_))
        .getOrElse(default)
    }

    // Parse URI — accept either "uri" or "path" (but not both)
    val uriOpt  = Option(dataSourceOptions.get(URI))
    val pathOpt = Option(dataSourceOptions.get(PATH))

    val uri = (uriOpt, pathOpt) match {
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException(
          s"Both '$URI' and '$PATH' options were provided. Please use only one of them."
        )
      case (Some(u), None) => u
      case (None, Some(p)) => p
      case (None, None) =>
        throw new IllegalArgumentException(
          s"Either '$URI' or '$PATH' option is required"
        )
    }

    // Parse durations with support for human-readable formats
    val pollInterval   = getDuration(POLL_INTERVAL, DEFAULT_POLLING_INTERVAL)
    val connectTimeout = getDuration(CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT)
    val readTimeout    = getDuration(READ_TIMEOUT, DEFAULT_READ_TIMEOUT)
    val retryDelay     = getDuration(RETRY_DELAY, DEFAULT_RETRY_DELAY)

    // Parse other options
    val numPartitions = dataSourceOptions.getInt(PARTITIONS, DEFAULT_NUM_PARTITIONS)
    val maxFiles      = dataSourceOptions.getInt(MAX_FILES, DEFAULT_MAX_FILES_PER_TRIGGER)

    // Parse and validate start offset mode
    val startOffset = Option(dataSourceOptions.get(START_OFFSET)).getOrElse(DEFAULT_START_OFFSET)
    StartOffsets.isValid(startOffset)

    val config = RemoteFileDataSourceOptions(
      remoteClient = dataSourceOptions.get(REMOTE_CLIENT),
      apiKey = Option(dataSourceOptions.get(API_KEY)),
      apiSecret = Option(dataSourceOptions.get(API_SECRET)),
      sessionId = Option(dataSourceOptions.get(SESSION_ID)),
      numPartitions = numPartitions,
      connectionTimeout = connectTimeout,
      readTimeout = readTimeout,
      maxRetries = dataSourceOptions.getInt(MAX_RETRIES, DEFAULT_MAX_RETRIES),
      retryDelay = retryDelay,
      enableSsl = dataSourceOptions.getBoolean(ENABLE_SSL, false),
      trustStorePath = Option(dataSourceOptions.get(TRUST_STORE_PATH)),
      trustStorePassword = Option(dataSourceOptions.get(TRUST_STORE_PASSWORD)),
      serverUri = uri,
      pollingInterval = pollInterval,
      maxFilesPerTrigger = maxFiles,
      asyncListFiles = dataSourceOptions.getBoolean(ASYNC_LIST_FILES, DEFAULT_ASYNC_LIST_FILES),
      asyncDownloads = dataSourceOptions.getBoolean(ASYNC_DOWNLOADS, DEFAULT_ASYNC_DOWNLOADS),
      asyncPrefetchSize = dataSourceOptions.getInt(ASYNC_PREFETCH_SIZE, DEFAULT_ASYNC_PREFETCH_SIZE),
      asyncDownloadThreads = dataSourceOptions.getInt(ASYNC_DOWNLOAD_THREADS, DEFAULT_ASYNC_DOWNLOAD_THREADS),
      startOffset = startOffset,
      allOptions = dataSourceOptions
    )

    // Log the parsed configuration at INFO level
    logDebug(config.toLogString)

    config
  }
}
