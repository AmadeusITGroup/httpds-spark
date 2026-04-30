package com.amadeus.spark.datasource.remote

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.net.http.HttpClient
import java.util.{HashMap => JHashMap}
import java.util.concurrent.ThreadPoolExecutor
import scala.concurrent.ExecutionContext

/**
 * Unit tests for ExecutorAsyncResources.
 */
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

      // Verify default thread count is 2x available processors
      val client   = ExecutorAsyncResources.getHttpClient(config)
      val executor = client.executor().get().asInstanceOf[ThreadPoolExecutor]
      executor.getCorePoolSize shouldBe Runtime.getRuntime.availableProcessors() * 2
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
      client.connectTimeout().get().toMillis shouldBe 10000L
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
}
