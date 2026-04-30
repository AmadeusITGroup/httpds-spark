package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.client.MockRemoteAsyncFileClient
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util

class RemoteFilePartitionReaderFactoryTest extends AnyFunSpec with Matchers {

  private val schema = RemoteFileFormat.SCHEMA

  private def optionsMap(async: Boolean, remoteClientName: String = "mock"): CaseInsensitiveStringMap = {
    val map = new util.HashMap[String, String]()
    map.put("remoteClient", remoteClientName)
    map.put("uri", "http://localhost:8080")
    map.put("asyncDownloads", async.toString)
    new CaseInsensitiveStringMap(map)
  }

  /** Helper to read a private field via reflection */
  private def getField[T](obj: AnyRef, fieldName: String): T = {
    val fields = Iterator
      .iterate[Class[_]](obj.getClass)(_.getSuperclass)
      .takeWhile(_ != null)
      .flatMap(_.getDeclaredFields)
    val field = fields
      .find(_.getName.endsWith(fieldName))
      .getOrElse(throw new NoSuchFieldException(s"$fieldName not found on ${obj.getClass}"))
    field.setAccessible(true)
    field.get(obj).asInstanceOf[T]
  }

  describe("createReader") {

    it("returns a RemoteFilePartitionReader when asyncDownloads is false") {
      val factory   = new RemoteFilePartitionReaderFactory(schema, optionsMap(async = false))
      val partition = RemoteFileInputPartition(Seq("file1.log"))

      val reader = factory.createReader(partition)

      reader shouldBe a[RemoteFilePartitionReader]
      getField[RemoteFileInputPartition](reader, "partition") shouldBe partition
      getField[StructType](reader, "schema") shouldBe schema
    }

    it("throws IllegalStateException when asyncDownloads is true but client does not support async") {
      val factory   = new RemoteFilePartitionReaderFactory(schema, optionsMap(async = true))
      val partition = RemoteFileInputPartition(Seq("file1.log"))

      an[IllegalStateException] should be thrownBy factory.createReader(partition)
    }

    it("returns a AsyncRemoteFilePartitionReader when  asyncDownloads is true") {
      val factory   = new RemoteFilePartitionReaderFactory(schema, optionsMap(async = true, remoteClientName = "mock-async"))
      val partition = RemoteFileInputPartition(Seq("file1.log"))

      val reader = factory.createReader(partition)

      reader shouldBe a[AsyncRemoteFilePartitionReader]
      getField[RemoteFileInputPartition](reader, "partition") shouldBe partition
      getField[StructType](reader, "schema") shouldBe schema
      getField[AnyRef](reader, "asyncClient") shouldBe a[MockRemoteAsyncFileClient]
    }
  }

  describe("supportColumnarReads") {

    it("returns false") {
      val factory   = new RemoteFilePartitionReaderFactory(schema, optionsMap(async = false))
      val partition = RemoteFileInputPartition(Seq("file1.log"))

      factory.supportColumnarReads(partition) shouldBe false
    }
  }
}
