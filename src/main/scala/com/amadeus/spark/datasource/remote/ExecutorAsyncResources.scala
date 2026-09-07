package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.http.HttpClient
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * Executor-scoped async resources, shared by tasks with the same thread count and connection timeout.
 * Different configurations get separate pools; resources live until executor shutdown.
 * Defaults and aliases are resolved by RemoteFileDataSourceOptions (4 threads, 30-second timeout).
 */
object ExecutorAsyncResources extends Logging {
  private case class ResourceKey(threads: Int, timeout: java.time.Duration)
  private case class Resources(executor: ExecutorService, executionContext: ExecutionContextExecutorService, httpClient: HttpClient)

  private val resources     = mutable.Map.empty[ResourceKey, Resources]
  private val threadCounter = new AtomicInteger()

  // Register once, including across explicit shutdown/reinitialization. Do not remove a running JVM hook.
  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdown(), "executor-async-resources-shutdown"))

  def getExecutionContext(config: java.util.Map[String, String]): ExecutionContext =
    getResources(RemoteFileDataSourceOptions.asyncSettings(new CaseInsensitiveStringMap(config))).executionContext

  def getHttpClient(config: java.util.Map[String, String]): HttpClient =
    getResources(RemoteFileDataSourceOptions.asyncSettings(new CaseInsensitiveStringMap(config))).httpClient

  def getExecutionContext(config: RemoteFileDataSourceOptions): ExecutionContext = getResources((config.asyncDownloadThreads, config.connectionTimeout)).executionContext

  def getHttpClient(config: RemoteFileDataSourceOptions): HttpClient = getResources((config.asyncDownloadThreads, config.connectionTimeout)).httpClient

  private def getResources(settings: (Int, Duration)): Resources = synchronized {
    val (threads, timeout) = settings
    require(threads > 0, "'asyncDownloadThreads' must be positive")
    require(timeout.isFinite && timeout.length > 0, "'connectTimeout' must be finite and positive")
    val key = ResourceKey(threads, java.time.Duration.ofNanos(timeout.toNanos))
    resources.getOrElseUpdate(key, createResources(key))
  }

  private def createResources(key: ResourceKey): Resources = {
    val executor = Executors.newFixedThreadPool(
      key.threads,
      new ThreadFactory {
        private val defaultFactory = Executors.defaultThreadFactory()

        override def newThread(runnable: Runnable): Thread = {
          val thread = defaultFactory.newThread(runnable)
          thread.setName(s"spark-remote-file-async-${threadCounter.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      }
    )
    try {
      val client = HttpClient
        .newBuilder()
        .executor(executor)
        .connectTimeout(key.timeout)
        .version(HttpClient.Version.HTTP_1_1)
        .build()
      logInfo(s"[EXECUTOR-RESOURCES] Created resources with ${key.threads} threads and ${key.timeout} connection timeout")
      Resources(executor, ExecutionContext.fromExecutorService(executor), client)
    } catch {
      case NonFatal(e) =>
        executor.shutdownNow()
        throw e
    }
  }

  /** Stops all pools, with a shared 30-second grace period. Safe to call repeatedly. */
  def shutdown(): Unit = synchronized {
    val pools = resources.values.map(_.executor).toVector
    resources.clear()
    pools.foreach(_.shutdown())
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    pools.foreach { executor =>
      try {
        if (!executor.awaitTermination(math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
          val _ = executor.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          executor.shutdownNow()
          Thread.currentThread().interrupt()
      }
    }
  }
}
