package com.amadeus.spark.datasource.remote

import org.apache.spark.internal.Logging

import java.net.http.HttpClient
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/**
 * Manages executor-scoped resources for asynchronous HTTP operations.
 *
 * This object is instantiated once per executor JVM and shared across all tasks.
 * It provides:
 * - Shared thread pool and ExecutionContext for async operations
 * - Shared HttpClient with unified connection pool
 *
 * **Why Shared HttpClient?**
 * - HttpClient internally maintains a connection pool
 * - Creating per-task clients wastes memory and prevents connection reuse
 * - Shared client enables connection pooling across all tasks on the executor
 * - Dramatically reduces TCP connection overhead for high-throughput scenarios
 *
 * **Lifecycle:**
 * - Initialized lazily on first access
 * - Registered with JVM shutdown hook for cleanup
 * - Thread-safe using double-checked locking pattern
 *
 * **Usage:**
 * - Called by PartitionReaderFactory to get ExecutionContext and HttpClient
 * - Both resources passed to clients that support async operations
 * - Shared across all partition readers in the executor
 *
 * **Configuration:**
 * - Thread pool size: spark.remoteFile.asyncDownload.threads (default: 2x cores for I/O)
 * - Connection timeout: spark.remoteFile.connectionTimeout (default: 10s)
 * - Threads are named for debugging: "spark-remote-file-async-N"
 * - Daemon threads to not block JVM shutdown
 */
object ExecutorAsyncResources extends Logging {

  @volatile private var initialized                             = false
  private var executorService: ExecutorService                  = _
  private var executionContext: ExecutionContextExecutorService = _
  private var httpClient: HttpClient                            = _

  // Shutdown hook for cleanup
  private val shutdownHook = new Thread(() => shutdown(), "executor-async-resources-shutdown")

  /**
   * Gets the shared ExecutionContext for this executor.
   *
   * Lazily initializes the thread pool and ExecutionContext on first access.
   * Thread-safe using double-checked locking pattern.
   *
   * @param config configuration map containing thread pool settings
   * @return ExecutionContext for async operations
   */
  def getExecutionContext(config: java.util.Map[String, String]): ExecutionContext = {
    if (!initialized) {
      synchronized {
        if (!initialized) {
          initialize(config)
        }
      }
    }
    executionContext
  }

  /**
   * Gets the shared HttpClient for this executor.
   *
   * Lazily initializes the HttpClient on first access.
   * Thread-safe using double-checked locking pattern.
   *
   * The returned HttpClient is configured for optimal performance:
   * - Uses the shared ExecutorService for async operations
   * - Configured connection timeout
   * - Internal connection pool shared across all tasks
   *
   * @param config configuration map containing timeout settings
   * @return HttpClient for async HTTP operations
   */
  def getHttpClient(config: java.util.Map[String, String]): HttpClient = {
    if (!initialized) {
      synchronized {
        if (!initialized) {
          initialize(config)
        }
      }
    }
    httpClient
  }

  /**
   * Initializes the executor resources.
   * Must be called within synchronized block.
   */
  private def initialize(config: java.util.Map[String, String]): Unit = {
    // 2x cores for I/O bound work (downloads spend most time waiting on network)
    val threads = Option(config.get("spark.remoteFile.asyncDownload.threads"))
      .map(_.toInt)
      .getOrElse(Runtime.getRuntime.availableProcessors() * 2)

    // Connection timeout in milliseconds (default: 10 seconds)
    val connectionTimeoutMs = Option(config.get("spark.remoteFile.connectionTimeout"))
      .map(_.toLong)
      .getOrElse(10000L)

    logInfo(s"[EXECUTOR-RESOURCES] Initializing async resources with $threads threads, ${connectionTimeoutMs}ms connection timeout")

    // Create thread pool for async operations
    executorService = Executors.newFixedThreadPool(
      threads,
      new ThreadFactory {
        private val counter        = new AtomicInteger(0)
        private val defaultFactory = Executors.defaultThreadFactory()

        override def newThread(r: Runnable): Thread = {
          val t = defaultFactory.newThread(r)
          t.setName(s"spark-remote-file-async-${counter.incrementAndGet()}")
          t.setDaemon(true) // Daemon threads don't prevent JVM shutdown
          t
        }
      }
    )

    executionContext = ExecutionContext.fromExecutorService(executorService)

    // Create shared HttpClient using the executor service
    // This enables connection pooling across all tasks on this executor
    httpClient = HttpClient
      .newBuilder()
      .executor(executorService) // Use shared thread pool for async operations
      .connectTimeout(java.time.Duration.ofMillis(connectionTimeoutMs))
      .version(HttpClient.Version.HTTP_1_1) // HTTP/1.1 for better compatibility
      .build()

    logInfo("[EXECUTOR-RESOURCES] Created shared HttpClient with connection pooling")

    // Register shutdown hook
    try {
      Runtime.getRuntime.addShutdownHook(shutdownHook)
      logDebug("[EXECUTOR-RESOURCES] Registered JVM shutdown hook")
    } catch {
      case e: IllegalStateException =>
        // Shutdown already in progress - this is fine
        logWarning("[EXECUTOR-RESOURCES] Could not register shutdown hook (shutdown in progress)")
    }

    initialized = true
    logInfo(s"[EXECUTOR-RESOURCES] Initialized successfully with $threads threads and shared HttpClient")
  }

  /**
   * Shuts down the executor resources gracefully.
   *
   * Called by JVM shutdown hook. Attempts graceful shutdown with timeout,
   * then forces shutdown if necessary.
   *
   * Shutdown order:
   * 1. Shutdown executor service (stops accepting new tasks)
   * 2. Wait for tasks to complete (30s grace period)
   * 3. HttpClient cleanup (handled automatically by underlying connection pool)
   *
   * This method is idempotent and thread-safe.
   */
  def shutdown(): Unit = synchronized {
    if (initialized) {
      logInfo("[EXECUTOR-RESOURCES] Shutting down async resources")

      executorService.shutdown()

      try {
        // Wait up to 30 seconds for graceful shutdown
        if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
          logWarning("[EXECUTOR-RESOURCES] Executor service did not terminate gracefully within 30s, forcing shutdown")
          val remainingTasks = executorService.shutdownNow()
          logWarning(s"[EXECUTOR-RESOURCES] Forced shutdown, ${remainingTasks.size} tasks were interrupted")
        } else {
          logInfo("[EXECUTOR-RESOURCES] Executor service terminated gracefully")
        }
      } catch {
        case _: InterruptedException =>
          logWarning("[EXECUTOR-RESOURCES] Interrupted during shutdown, forcing shutdown now")
          executorService.shutdownNow()
          Thread.currentThread().interrupt()
      }

      // Note: HttpClient doesn't have an explicit close() method
      // Its connection pool is managed by the underlying executor service
      // which we've already shut down above
      logInfo("[EXECUTOR-RESOURCES] HttpClient connection pool will be cleaned up automatically")

      initialized = false
      logInfo("[EXECUTOR-RESOURCES] Shutdown complete")
    }
  }
}
