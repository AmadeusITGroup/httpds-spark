package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.client.MockRemoteAsyncFileClient
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util
import java.net.http.HttpClient
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

// Spark/Java API reflection tests require asInstanceOf casts for type-erased Java generics
// scalafix:off DisableSyntax.asInstanceOf
class RemoteFilePartitionReaderFactoryTest extends AnyFunSpec with Matchers {

  private val schema = RemoteFileFormat.SCHEMA

  private def optionsMap(async: Boolean, remoteClientName: String = "mock", extra: Map[String, String] = Map.empty): CaseInsensitiveStringMap = {
    val map = new util.HashMap[String, String]()
    map.put("remoteClient", remoteClientName)
    map.put("uri", "http://localhost:8080")
    map.put("asyncDownloads", async.toString)
    extra.foreach { case (key, value) => map.put(key, value) }
    new CaseInsensitiveStringMap(map)
  }

  /** Helper to read a private field via reflection */
  private def getField[T](obj: AnyRef, fieldName: String): T = {
    val fields = Iterator
      .iterate[Class[_]](obj.getClass)(_.getSuperclass)
      // Java reflection Class.getSuperclass() returns null at root — required Java interop
      // scalafix:off DisableSyntax.null
      .takeWhile(_ != null)
      // scalafix:on DisableSyntax.null
      .flatMap(_.getDeclaredFields)
    val field = fields
      .find(_.getName.endsWith(fieldName))
      .getOrElse(throw new NoSuchFieldException(s"$fieldName not found on ${obj.getClass}"))
    field.setAccessible(true)
    field.get(obj).asInstanceOf[T]
  }

  describe("createReader") {

    it("passes public settings to clients and isolates different query configurations") {
      val readers = Seq(
        Map("ASYNCDOWNLOADTHREADS" -> "2", "CONNECTTIMEOUT"    -> "5s"),
        Map("asyncDownloadThreads" -> "3", "connectionTimeout" -> "7s"),
        Map("asyncDownloadThreads" -> "2", "connectTimeout"    -> "5000ms")
      ).map { extra =>
        val factory = new RemoteFilePartitionReaderFactory(schema, optionsMap(async = true, remoteClientName = classOf[ResourceCapturingClient].getName, extra = extra))
        factory.createReader(RemoteFileInputPartition(Seq.empty))
      }
      try {
        val clients = readers.map(reader => getField[ResourceCapturingClient](reader, "asyncClient").receivedClient.get())
        clients(0).connectTimeout().get().toMillis shouldBe 5000L
        clients(0).executor().get().asInstanceOf[ThreadPoolExecutor].getCorePoolSize shouldBe 2
        clients(1).connectTimeout().get().toMillis shouldBe 7000L
        clients(1).executor().get().asInstanceOf[ThreadPoolExecutor].getCorePoolSize shouldBe 3
        clients(1) should not be theSameInstanceAs(clients(0))
        clients(2) shouldBe theSameInstanceAs(clients(0))
      } finally readers.foreach(_.close())
    }

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

// scalafix:on DisableSyntax.asInstanceOf

class ResourceCapturingClient extends MockRemoteAsyncFileClient {
  val receivedClient                                                                       = new AtomicReference[HttpClient]()
  override def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit = receivedClient.set(httpClient)
}
