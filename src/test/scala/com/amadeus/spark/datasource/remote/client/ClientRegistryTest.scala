package com.amadeus.spark.datasource.remote.client

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for ClientRegistry.
 */
class ClientRegistryTest extends AnyFunSpec with Matchers {

  describe("lookupDataSource") {

    describe("when looking up by short name") {

      it("should find client by short name 'mock'") {
        val clientClass = ClientRegistry.lookupDataSource("mock")
        clientClass should not be null
        clientClass.getName shouldBe "com.amadeus.spark.datasource.remote.client.MockRemoteFileClient"
      }

      //it("should find client by short name 'imperva'") {
      //  val clientClass = ClientRegistry.lookupDataSource("imperva")
      //  clientClass should not be null
      //  clientClass.getName shouldBe "com.imperva.spark.datasource.client.IncapsulaClient"
      //}

      it("should be case-insensitive when looking up by short name") {
        val clientClass1 = ClientRegistry.lookupDataSource("MOCK")
        val clientClass2 = ClientRegistry.lookupDataSource("Mock")
        val clientClass3 = ClientRegistry.lookupDataSource("mock")

        clientClass1 shouldBe clientClass2
        clientClass2 shouldBe clientClass3
        clientClass1.getName shouldBe "com.amadeus.spark.datasource.remote.client.MockRemoteFileClient"
      }
    }

    describe("when looking up by fully qualified class name") {

      it("should find MockRemoteFileClient by fully qualified class name") {
        val clientClass = ClientRegistry.lookupDataSource("com.amadeus.spark.datasource.remote.client.MockRemoteFileClient")
        clientClass should not be null
        clientClass.getName shouldBe "com.amadeus.spark.datasource.remote.client.MockRemoteFileClient"
      }

      it("should fall back to class name loading when short name is not found") {
        // Using a class that exists but is not registered with a short name
        val clientClass = ClientRegistry.lookupDataSource("com.amadeus.spark.datasource.remote.client.RemoteFileClient")
        clientClass should not be null
        clientClass.getName shouldBe "com.amadeus.spark.datasource.remote.client.RemoteFileClient"
      }
    }

    describe("when client is not found") {

      it("should throw ClassNotFoundException for non-existent short name") {
        val exception = intercept[ClassNotFoundException] {
          ClientRegistry.lookupDataSource("nonexistent")
        }
        exception.getMessage should include("nonexistent")
      }

      it("should throw ClassNotFoundException for non-existent fully qualified class name") {
        val exception = intercept[ClassNotFoundException] {
          ClientRegistry.lookupDataSource("com.example.NonExistentClient")
        }
        exception.getMessage should include("com.example.NonExistentClient")
      }
    }

    describe("when multiple clients have the same short name") {

      it("should prefer internal client over external when both exist") {
        val clientClass = ClientRegistry.lookupDataSource("mock-test")
        clientClass should not be null
        clientClass.getName shouldBe "com.amadeus.spark.datasource.remote.client.MockForTestsRemoteFileClient"
      }

      it("should throw IllegalArgumentException when multiple clients found with no internal preference") {
        val exception = intercept[IllegalArgumentException] {
          ClientRegistry.lookupDataSource("mock-fail-test")
        }
        exception.getMessage should include("com.clientregistrytest.MockForExternalFailSecTestsRemoteFileClient, com.clientregistrytest.MockForExternalFailTestsRemoteFileClient")
      }
    }
  }
}
