package com.amadeus.spark.datasource.remote.client

import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.clientregistrytest.{MockForExternalTestsRemoteFileClient, MockForExternalTestsRemoteFileClientSorter}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.collection.JavaConverters._

/**
 * Unit tests for RemoteFileClient companion object.
 */
class RemoteFileClientTest extends AnyFunSpec with Matchers {

  describe("from") {

    it("should instantiate MockRemoteFileClient with RemoteFileDataSourceOptions options constructor") {
      val options = createOptions("mock-test")
      val client  = RemoteFileClient.from(options)

      client should not be null
      client shouldBe a[MockForTestsRemoteFileClient]
      val mockClient = client.asInstanceOf[MockForTestsRemoteFileClient]
      client.shortName() shouldBe "mock-test"
      mockClient.options shouldBe options

    }

    it("should instantiate client with empty constructor and call init") {
      val options = createOptions("mock")
      val client  = RemoteFileClient.from(options)

      client should not be null
      client shouldBe a[MockRemoteFileClient]
      val mockClient = client.asInstanceOf[MockRemoteFileClient]
      client.shortName() shouldBe "mock"
      mockClient.options shouldBe options
    }

    it("should throw ClassNotFoundException for non-existent client") {
      val options = createOptions("non-existent-client")

      val exception = intercept[ClassNotFoundException] {
        RemoteFileClient.from(options)
      }

      exception.getMessage should include("non-existent-client")
    }

  }

  describe("listAndSortLogFiles") {
    it("should sort files by fetchedAt when client does not implement FileSorter") {
      val options = createOptions("com.clientregistrytest.MockForExternalTestsRemoteFileClient")
      val client  = RemoteFileClient.from(options)

      client should not be null
      client shouldBe a[MockForExternalTestsRemoteFileClient]

      // MockRemoteFileClient returns files with different fetchedAt timestamps
      val sortedFiles = client.listAndSortLogFiles()

      val expected = Seq(
        RemoteFile(name = "Log entry 2", fetchedAt = Timestamp.valueOf("2024-01-01 10:00:00")),
        RemoteFile(name = "Log entry 3", fetchedAt = Timestamp.valueOf("2024-01-01 10:28:00")),
        RemoteFile(name = "Log entry 1", fetchedAt = Timestamp.valueOf("2024-01-01 10:50:00"))
      )

      sortedFiles shouldEqual expected
    }

    it("should sort files using custom sorter when client implements FileSorter") {
      val options = createOptions("mock-test-sorter")
      val client  = RemoteFileClient.from(options)

      client should not be null
      client shouldBe a[MockForExternalTestsRemoteFileClientSorter]

      // MockForTestsRemoteFileClient implements FileSorter and sorts by filename
      val sortedFiles = client.listAndSortLogFiles()

      val expected = Seq(
        RemoteFile(name = "Log entry 3", fetchedAt = Timestamp.valueOf("2024-01-01 10:50:00")),
        RemoteFile(name = "Log entry 2", fetchedAt = Timestamp.valueOf("2024-01-01 10:55:00")),
        RemoteFile(name = "Log entry 1", fetchedAt = Timestamp.valueOf("2024-01-01 10:28:00"))
      )

      sortedFiles shouldEqual expected
    }
  }

  /**
   * Helper method to create RemoteFileDataSourceOptions with a specific remote client.
   *
   * @param remoteClient the remote client name or class
   * @return RemoteFileDataSourceOptions instance
   */
  private def createOptions(remoteClient: String): RemoteFileDataSourceOptions = {
    val optionsMap = Map(
      "uri"          -> "http://localhost:5000",
      "remoteClient" -> remoteClient
    ).asJava

    RemoteFileDataSourceOptions.fromMap(new CaseInsensitiveStringMap(optionsMap))
  }
}
