package com.amadeus.spark.datasource.remote

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.net.http.HttpClient
import java.util.concurrent.ThreadPoolExecutor
import java.util.{HashMap => JHashMap}
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

/**
 * Unit tests for ExecutorAsyncResources.
 */
// ScalaTest assertions and Java API checks require null comparisons — Java interop test patterns
// scalafix:off DisableSyntax.null
// Spark/Java API reflection tests require asInstanceOf casts for type-erased Java generics
// scalafix:off DisableSyntax.asInstanceOf
class ExecutorAsyncResourcesTest extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    // Ensure resources are cleaned up between tests
    ExecutorAsyncResources.shutdown()
    super.afterEach()
  }

  describe("getExecutionContext") {

    it("should return an ExecutionContext with default config") {
      val config           = new JHashMap[String, String]()
      val executionContext = ExecutorAsyncResources.getExecutionContext(config)
      executionContext should not be null
      executionContext shouldBe an[ExecutionContext]

      // Defaults match the public datasource options.
      val client   = ExecutorAsyncResources.getHttpClient(config)
      val executor = client.executor().get().asInstanceOf[ThreadPoolExecutor]
      executor.getCorePoolSize shouldBe 4
    }

    it("should return the same ExecutionContext on subsequent calls") {
      val config            = new JHashMap[String, String]()
      val executionContext1 = ExecutorAsyncResources.getExecutionContext(config)
      val executionContext2 = ExecutorAsyncResources.getExecutionContext(config)
      executionContext1 shouldBe theSameInstanceAs(executionContext2)
    }

    it("should use custom thread count from config") {
      val config = new JHashMap[String, String]()
      config.put("spark.remoteFile.asyncDownload.threads", "4")
      val executionContext = ExecutorAsyncResources.getExecutionContext(config)
      executionContext should not be null

      val client   = ExecutorAsyncResources.getHttpClient(config)
      val executor = client.executor().get().asInstanceOf[ThreadPoolExecutor]
      executor.getCorePoolSize shouldBe 4
    }
  }

  describe("getHttpClient") {

    it("should return an HttpClient with default config") {
      val config = new JHashMap[String, String]()
      val client = ExecutorAsyncResources.getHttpClient(config)

      client should not be null
      client.connectTimeout().get().toMillis shouldBe 30000L
      client.version() shouldBe HttpClient.Version.HTTP_1_1
      client.executor().isPresent shouldBe true
    }

    it("should return the same HttpClient on subsequent calls") {
      val config  = new JHashMap[String, String]()
      val client1 = ExecutorAsyncResources.getHttpClient(config)
      val client2 = ExecutorAsyncResources.getHttpClient(config)
      client1 shouldBe theSameInstanceAs(client2)
    }

    it("should use custom connection timeout from config") {
      val config = new JHashMap[String, String]()
      config.put("spark.remoteFile.connectionTimeout", "5000")
      val client = ExecutorAsyncResources.getHttpClient(config)

      client should not be null
      client.connectTimeout().get().toMillis shouldBe 5000L
      client.version() shouldBe HttpClient.Version.HTTP_1_1
      client.executor().isPresent shouldBe true
    }
  }

  describe("shutdown") {

    it("should shut down every configuration pool") {
      val first  = ExecutorAsyncResources.getHttpClient(Map("asyncDownloadThreads" -> "2").asJava)
      val second = ExecutorAsyncResources.getHttpClient(Map("asyncDownloadThreads" -> "3").asJava)
      ExecutorAsyncResources.shutdown()
      first.executor().get().asInstanceOf[ThreadPoolExecutor].isShutdown shouldBe true
      second.executor().get().asInstanceOf[ThreadPoolExecutor].isShutdown shouldBe true
    }

    it("should be idempotent - calling shutdown multiple times should not fail") {
      val config = new JHashMap[String, String]()
      ExecutorAsyncResources.getExecutionContext(config)

      ExecutorAsyncResources.shutdown()
      noException should be thrownBy ExecutorAsyncResources.shutdown()
    }

    it("should allow re-initialization after shutdown") {
      val config            = new JHashMap[String, String]()
      val executionContext1 = ExecutorAsyncResources.getExecutionContext(config)
      ExecutorAsyncResources.shutdown()

      val executionContext2 = ExecutorAsyncResources.getExecutionContext(config)
      executionContext2 should not be null
      // After shutdown and re-init, should be a new instance
      executionContext1 should not be theSameInstanceAs(executionContext2)
    }

    it("should not fail when called without prior initialization") {
      noException should be thrownBy ExecutorAsyncResources.shutdown()
    }
  }

  describe("configuration sharing") {
    it("should normalize equivalent settings and reuse resources despite different query options") {
      val first  = Map("asyncDownloadThreads" -> "2", "connectTimeout" -> "5s", "uri" -> "http://one").asJava
      val second = Map("ASYNCDOWNLOADTHREADS" -> "2", "CONNECTIONTIMEOUT" -> "5000ms", "uri" -> "http://two").asJava
      ExecutorAsyncResources.getHttpClient(first) shouldBe theSameInstanceAs(ExecutorAsyncResources.getHttpClient(second))
      ExecutorAsyncResources.getExecutionContext(first) shouldBe theSameInstanceAs(ExecutorAsyncResources.getExecutionContext(second))
    }

    it("should isolate a change to either threads or timeout without replacing existing resources") {
      val original       = Map("asyncDownloadThreads" -> "2", "connectTimeout" -> "5s").asJava
      val first          = ExecutorAsyncResources.getHttpClient(original)
      val threadsChanged = ExecutorAsyncResources.getHttpClient(Map("asyncDownloadThreads" -> "3", "connectTimeout" -> "5s").asJava)
      val timeoutChanged = ExecutorAsyncResources.getHttpClient(Map("asyncDownloadThreads" -> "2", "connectTimeout" -> "6s").asJava)
      threadsChanged should not be theSameInstanceAs(first)
      timeoutChanged should not be theSameInstanceAs(first)
      threadsChanged.executor().get().asInstanceOf[ThreadPoolExecutor].getCorePoolSize shouldBe 3
      timeoutChanged.connectTimeout().get().toMillis shouldBe 6000L
      ExecutorAsyncResources.getHttpClient(original) shouldBe theSameInstanceAs(first)
      first.executor().get().asInstanceOf[ThreadPoolExecutor].isShutdown shouldBe false
    }

    it("should create a single shared client under concurrent access") {
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val config                                      = Map("asyncDownloadThreads" -> "2").asJava
      val clients                                     = Await.result(Future.sequence((1 to 16).map(_ => Future(ExecutorAsyncResources.getHttpClient(config)))), 5.seconds)
      clients.foreach(_ shouldBe theSameInstanceAs(clients.head))
    }
  }
}
// scalafix:on DisableSyntax.asInstanceOf
// scalafix:on DisableSyntax.null
