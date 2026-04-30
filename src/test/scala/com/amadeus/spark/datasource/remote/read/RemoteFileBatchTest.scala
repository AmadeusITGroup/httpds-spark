package com.amadeus.spark.datasource.remote.read

import com.amadeus.spark.datasource.remote.RemoteFileInputPartition
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.amadeus.spark.datasource.remote.RemoteFilePartitionReaderFactory

import scala.collection.JavaConverters._

class RemoteFileBatchTest extends AnyFunSpec with Matchers {

  private val schema = StructType(Seq(StructField("value", StringType)))

  private def createBatch(remoteClient: String, numPartitions: Int = 4): RemoteFileBatch = {
    val optionsMap = Map(
      "uri"          -> "http://localhost:5000",
      "remoteClient" -> remoteClient,
      "partitions"   -> numPartitions.toString
    ).asJava
    new RemoteFileBatch(schema, new CaseInsensitiveStringMap(optionsMap))
  }

  describe("planInputPartitions") {

    it("should return empty array when no files are found") {
      // "mock-test" client returns no files
      val batch      = createBatch("mock-test")
      val partitions = batch.planInputPartitions()

      partitions shouldBe empty
    }

    it("should create partitions with files distributed in round-robin") {
      // "mock" client returns file1.log, file2.log, file3.log
      val batch      = createBatch("mock", numPartitions = 2)
      val partitions = batch.planInputPartitions()

      partitions should have length 2
      val p0 = partitions(0).asInstanceOf[RemoteFileInputPartition]
      val p1 = partitions(1).asInstanceOf[RemoteFileInputPartition]

      // Files are sorted then round-robin distributed
      p0.files should contain("file1.log")
      p0.files should contain("file3.log")
      p1.files should contain("file2.log")
    }

    it("should limit partitions to the number of files when fewer files than partitions") {
      // "mock" client returns 3 files, request 10 partitions
      val batch      = createBatch("mock", numPartitions = 10)
      val partitions = batch.planInputPartitions()

      partitions should have length 3
    }
  }

  describe("createReaderFactory") {

    it("should return a RemoteFilePartitionReaderFactory") {
      val batch   = createBatch("mock")
      val factory = batch.createReaderFactory()

      factory shouldBe a[RemoteFilePartitionReaderFactory]

      val readerFactory = factory.asInstanceOf[RemoteFilePartitionReaderFactory]

      // Check schema via reflection
      val schemaField = classOf[RemoteFilePartitionReaderFactory].getDeclaredField("schema")
      schemaField.setAccessible(true)
      schemaField.get(readerFactory) shouldBe schema

      // Check optionsMap via reflection
      val optionsMapField = classOf[RemoteFilePartitionReaderFactory].getDeclaredField("optionsMap")
      optionsMapField.setAccessible(true)
      val storedOptions = optionsMapField.get(readerFactory).asInstanceOf[java.util.Map[String, String]]
      storedOptions.get("uri") shouldBe "http://localhost:5000"
      storedOptions.get("remoteClient") shouldBe "mock"
      storedOptions.get("partitions") shouldBe "4"
    }
  }
}
