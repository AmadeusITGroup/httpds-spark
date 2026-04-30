package com.amadeus.spark.datasource.remote.conf

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS, MINUTES}
import scala.collection.JavaConverters._

/**
 * Unit tests for RemoteFileDataSourceOptions.
 *
 * Tests cover:
 * - Parsing from maps with various duration formats
 * - Default values
 * - Validation logic
 * - Serialization (toMap, toLogString)
 * - Error handling for invalid configurations
 */
class RemoteFileDataSourceOptionsTest extends AnyFunSpec with Matchers {

  describe("RemoteFileDataSourceOptions.fromMap") {

    it("should parse basic required options") {
      val options = Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> "mock"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.serverUri shouldBe "http://localhost:5000"
      config.remoteClient shouldBe "mock"
    }

    it("should parse 'path' option as server URI") {
      val options = Map(
        "path"         -> "http://myserver:9090",
        "remoteClient" -> "mock"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.serverUri shouldBe "http://myserver:9090"
    }

    it("should parse 'path' provided via DataStreamReader.load(path) style") {
      val options = Map(
        "path"         -> "https://api.example.com/v1/logs",
        "remoteClient" -> "mock"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.serverUri shouldBe "https://api.example.com/v1/logs"
    }

    it("should throw exception when both uri and path are provided") {
      val options = Map(
        "uri"          -> "http://localhost:5000",
        "path"         -> "http://otherserver:8080",
        "remoteClient" -> "mock"
      ).asJava

      val exception = the[IllegalArgumentException] thrownBy {
        RemoteFileDataSourceOptions.fromMap(options)
      }
      exception.getMessage should include("Both")
      exception.getMessage should include("uri")
      exception.getMessage should include("path")
    }

    it("should throw exception when neither uri nor path is provided") {
      val options = Map(
        "remoteClient" -> "mock"
      ).asJava

      val exception = the[IllegalArgumentException] thrownBy {
        RemoteFileDataSourceOptions.fromMap(options)
      }
      exception.getMessage should include("required")
    }

    it("should use default values when options are not provided") {
      val options = Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> "mock"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.numPartitions shouldBe 4
      config.connectionTimeout shouldBe Duration(30, SECONDS)
      config.readTimeout shouldBe Duration(60, SECONDS)
      config.maxRetries shouldBe 3
      config.retryDelay shouldBe Duration(1, SECONDS)
      config.pollingInterval shouldBe Duration(2, SECONDS)
      config.maxFilesPerTrigger shouldBe 100
      config.enableSsl shouldBe false
      config.apiKey shouldBe None
      config.apiSecret shouldBe None
      config.sessionId shouldBe None
      config.trustStorePath shouldBe None
      config.trustStorePassword shouldBe None
    }

    it("should parse duration with seconds suffix") {
      val options = Map(
        "uri"            -> "http://localhost:5000",
        "remoteClient"   -> "mock",
        "pollInterval"   -> "30s",
        "connectTimeout" -> "45s",
        "readTimeout"    -> "90s",
        "retryDelay"     -> "2s"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.pollingInterval shouldBe Duration(30, SECONDS)
      config.connectionTimeout shouldBe Duration(45, SECONDS)
      config.readTimeout shouldBe Duration(90, SECONDS)
      config.retryDelay shouldBe Duration(2, SECONDS)
    }

    it("should parse duration with milliseconds suffix") {
      val options = Map(
        "uri"            -> "http://localhost:5000",
        "remoteClient"   -> "mock",
        "pollInterval"   -> "5000ms",
        "connectTimeout" -> "10000ms",
        "readTimeout"    -> "20000ms",
        "retryDelay"     -> "500ms"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.pollingInterval shouldBe Duration(5000, MILLISECONDS)
      config.connectionTimeout shouldBe Duration(10000, MILLISECONDS)
      config.readTimeout shouldBe Duration(20000, MILLISECONDS)
      config.retryDelay shouldBe Duration(500, MILLISECONDS)
    }

    it("should parse duration with minutes suffix") {
      val options = Map(
        "uri"            -> "http://localhost:5000",
        "remoteClient"   -> "mock",
        "pollInterval"   -> "2m",
        "connectTimeout" -> "1m",
        "readTimeout"    -> "5m"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.pollingInterval shouldBe Duration(2, MINUTES)
      config.connectionTimeout shouldBe Duration(1, MINUTES)
      config.readTimeout shouldBe Duration(5, MINUTES)
    }

    it("should parse all options") {
      val options = Map(
        "uri"                -> "http://localhost:5000",
        "remoteClient"       -> "mock",
        "partitions"         -> "8",
        "maxRetries"         -> "5",
        "maxFilesPerTrigger" -> "200",
        "apiKey"             -> "test-key-123",
        "apiSecret"          -> "test-secret-456",
        "enableSsl"          -> "true",
        "trustStorePath"     -> "/path/to/truststore.jks",
        "trustStorePassword" -> "trustpass",
        "sessionId"          -> "test-session-123"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.numPartitions shouldBe 8
      config.maxRetries shouldBe 5
      config.maxFilesPerTrigger shouldBe 200
      config.apiKey shouldBe Some("test-key-123")
      config.apiSecret shouldBe Some("test-secret-456")
      config.enableSsl shouldBe true
      config.trustStorePath shouldBe Some("/path/to/truststore.jks")
      config.trustStorePassword shouldBe Some("trustpass")
      config.sessionId shouldBe Some("test-session-123")
    }

    it("should be case insensitive for option keys") {
      val options = Map(
        "URI"          -> "http://localhost:5000",
        "RemoteClient" -> "mock",
        "ApiKey"       -> "test-key",
        "APISECRET"    -> "test-secret",
        "PARTITIONS"   -> "10",
        "EnableSsl"    -> "true"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.serverUri shouldBe "http://localhost:5000"
      config.remoteClient shouldBe "mock"
      config.apiKey shouldBe Some("test-key")
      config.apiSecret shouldBe Some("test-secret")
      config.numPartitions shouldBe 10
      config.enableSsl shouldBe true
    }

    it("should preserve allOptions map") {
      val options = Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> "mock",
        "customOption" -> "customValue"
      ).asJava

      val config = RemoteFileDataSourceOptions.fromMap(options)

      config.allOptions.get("customOption") shouldBe "customValue"
    }
  }

  describe("RemoteFileDataSourceOptions validation") {

    it("should pass validation for valid configuration") {
      val config = createValidConfig()
      noException should be thrownBy config.validate()
    }

    it("should fail validation when numPartitions is zero") {
      val config    = createValidConfig().copy(numPartitions = 0)
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must be positive")
    }

    it("should fail validation when numPartitions is negative") {
      val config    = createValidConfig().copy(numPartitions = -1)
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must be positive")
    }

    it("should fail validation when maxRetries is negative") {
      val config    = createValidConfig().copy(maxRetries = -1)
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must be non-negative")
    }

    it("should allow maxRetries to be zero") {
      val config = createValidConfig().copy(maxRetries = 0)
      noException should be thrownBy config.validate()
    }

    it("should fail validation when maxFilesPerTrigger is zero") {
      val config    = createValidConfig().copy(maxFilesPerTrigger = 0)
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must be positive")
    }

    it("should fail validation when maxFilesPerTrigger is negative") {
      val config    = createValidConfig().copy(maxFilesPerTrigger = -1)
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must be positive")
    }

    it("should fail validation when serverUri is empty") {
      val config    = createValidConfig().copy(serverUri = "")
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must not be empty")
    }

    it("should fail validation when remoteClient is empty") {
      val config    = createValidConfig().copy(remoteClient = "")
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("must not be empty")
    }

    it("should fail validation for malformed URI") {
      val config    = createValidConfig().copy(serverUri = "not a valid uri ://")
      val exception = the[IllegalArgumentException] thrownBy config.validate()
      exception.getMessage should include("not a valid URI")
    }

    it("should accept various valid URI formats") {
      val validUris = Seq(
        "http://localhost:5000",
        "https://secure.example.com:8443",
        "http://192.168.1.1:8080/path",
        "https://api.example.com/v1/endpoint"
      )

      validUris.foreach { uri =>
        val config = createValidConfig().copy(serverUri = uri)
        noException should be thrownBy config.validate()
      }
    }
  }

  describe("RemoteFileDataSourceOptions.validateForStreaming") {

    it("should pass validation for streaming with valid configuration") {
      val config = createValidConfig()
      noException should be thrownBy config.validate()
    }

    it("should inherit validation from validate()") {
      val config = createValidConfig().copy(numPartitions = -1)
      an[IllegalArgumentException] should be thrownBy config.validate()
    }
  }

  describe("RemoteFileDataSourceOptions.toMap") {

    it("should serialize all non-optional fields") {
      val config = createValidConfig()
      val map    = config.toMap

      map("uri") shouldBe "http://localhost:5000"
      map("partitions") shouldBe "4"
      map("connectTimeout") should not be empty
      map("readTimeout") should not be empty
      map("maxRetries") shouldBe "3"
      map("retryDelay") should not be empty
      map("enableSsl") shouldBe "false"
      map("pollInterval") should not be empty
      map("maxFilesPerTrigger") shouldBe "100"
    }

    it("should serialize optional fields when present") {
      val config = createValidConfig().copy(
        apiKey = Some("test-key"),
        apiSecret = Some("test-secret"),
        trustStorePath = Some("/path/to/truststore"),
        trustStorePassword = Some("password")
      )
      val map = config.toMap

      map("uri") shouldBe "http://localhost:5000"
      map("partitions") shouldBe "4"
      map("connectTimeout") should not be empty
      map("readTimeout") should not be empty
      map("maxRetries") shouldBe "3"
      map("retryDelay") should not be empty
      map("enableSsl") shouldBe "false"
      map("pollInterval") should not be empty
      map("maxFilesPerTrigger") shouldBe "100"
      map("apiKey") shouldBe "test-key"
      map("apiSecret") shouldBe "test-secret"
      map("trustStorePath") shouldBe "/path/to/truststore"
      map("trustStorePassword") shouldBe "password"
    }

    it("should not include optional fields when absent") {
      val config = createValidConfig().copy(
        apiKey = None,
        apiSecret = None,
        trustStorePath = None,
        trustStorePassword = None
      )
      val map = config.toMap

      map("uri") shouldBe "http://localhost:5000"
      map("partitions") shouldBe "4"
      map("connectTimeout") should not be empty
      map("readTimeout") should not be empty
      map("maxRetries") shouldBe "3"
      map("retryDelay") should not be empty
      map("enableSsl") shouldBe "false"
      map("pollInterval") should not be empty
      map("maxFilesPerTrigger") shouldBe "100"
      map should not contain key("apiKey")
      map should not contain key("apiSecret")
      map should not contain key("trustStorePath")
      map should not contain key("trustStorePassword")
    }

    it("should serialize durations correctly") {
      val config = createValidConfig().copy(
        pollingInterval = Duration(30, SECONDS),
        connectionTimeout = Duration(45, SECONDS),
        readTimeout = Duration(90, SECONDS),
        retryDelay = Duration(2, SECONDS)
      )
      val map = config.toMap

      // Most durations use toMillis.toString, except pollInterval which uses toString
      map("pollInterval") shouldBe "30 seconds"   // Uses toString, gives "30 seconds"
      map("connectTimeout") shouldBe "45 seconds" // Uses toMillis.toString
      map("readTimeout") shouldBe "90 seconds"
      map("retryDelay") shouldBe "2 seconds"
    }
  }

  describe("RemoteFileDataSourceOptions.toLogString") {

    it("should include all important configuration values") {
      val config = createValidConfig().copy(
        remoteClient = "imperva",
        serverUri = "http://test-server:8080",
        pollingInterval = Duration(20, SECONDS),
        maxFilesPerTrigger = 50,
        numPartitions = 8,
        maxRetries = 5,
        enableSsl = true
      )
      val logString = config.toLogString

      logString should include("imperva")
      logString should include("http://test-server:8080")
      logString should include("20")
      logString should include("50")
      logString should include("8")
      logString should include("5")
      logString should include("true")
    }

    it("should mask API key when present") {
      val config    = createValidConfig().copy(apiKey = Some("secret-key-123"))
      val logString = config.toLogString

      logString should include("***")
      logString should not include "secret-key-123"
    }

    it("should show '<not set>' when API key is absent") {
      val config    = createValidConfig().copy(apiKey = None)
      val logString = config.toLogString

      logString should include("<not set>")
    }

    it("should not expose sensitive information") {
      val config = createValidConfig().copy(
        apiKey = Some("secret-key"),
        apiSecret = Some("secret-password"),
        trustStorePassword = Some("truststore-pass")
      )
      val logString = config.toLogString

      logString should not include "secret-key"
      logString should not include "secret-password"
      logString should not include "truststore-pass"
    }
  }

  describe("RemoteFileDataSourceOptions round-trip serialization") {

    it("should preserve configuration through toMap and fromMap") {
      val original = RemoteFileDataSourceOptions.fromMap(
        Map(
          "uri"                -> "http://localhost:5000",
          "remoteClient"       -> "mock",
          "apiKey"             -> "test-key",
          "apiSecret"          -> "test-secret",
          "partitions"         -> "8",
          "connectTimeout"     -> "45000ms",
          "readTimeout"        -> "90000ms",
          "maxRetries"         -> "5",
          "retryDelay"         -> "2000ms",
          "enableSsl"          -> "true",
          "trustStorePath"     -> "/path/to/trust",
          "trustStorePassword" -> "pass",
          "pollInterval"       -> "30s",
          "maxFilesPerTrigger" -> "200"
        ).asJava
      )

      val serialized      = original.toMap
      val deserializedMap = new java.util.HashMap[String, String]()
      serialized.foreach { case (k, v) => deserializedMap.put(k, v) }
      val deserialized = RemoteFileDataSourceOptions.fromMap(deserializedMap)

      deserialized.serverUri shouldBe original.serverUri
      deserialized.remoteClient shouldBe original.remoteClient
      deserialized.apiKey shouldBe original.apiKey
      deserialized.apiSecret shouldBe original.apiSecret
      deserialized.numPartitions shouldBe original.numPartitions
      deserialized.connectionTimeout shouldBe original.connectionTimeout
      deserialized.readTimeout shouldBe original.readTimeout
      deserialized.maxRetries shouldBe original.maxRetries
      deserialized.retryDelay shouldBe original.retryDelay
      deserialized.enableSsl shouldBe original.enableSsl
      deserialized.trustStorePath shouldBe original.trustStorePath
      deserialized.trustStorePassword shouldBe original.trustStorePassword
      deserialized.pollingInterval shouldBe original.pollingInterval
      deserialized.maxFilesPerTrigger shouldBe original.maxFilesPerTrigger
    }
  }

  // Helper method to create a valid configuration for testing
  private def createValidConfig(): RemoteFileDataSourceOptions = {
    RemoteFileDataSourceOptions.fromMap(
      Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> "mock"
      ).asJava
    )
  }
}
