package com.amadeus.spark.datasource.remote.helpers

import com.amadeus.spark.datasource.remote.client._
import com.amadeus.spark.datasource.remote.{RemoteFileFormat, RemoteFileInputPartition}
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class RemoteFilePartitionReaderHelperTest extends AnyFunSpec with Matchers {

  val partition: RemoteFileInputPartition = RemoteFileInputPartition(Seq("file1.log", "file2.log"))
  val remoteFile                          = RemoteFile("file.log", Timestamp.valueOf("2026-05-05 00:00:00"))

  /** Minimal concrete implementation */
  class TestReader(downloadResults: List[DownloadResult]) extends RemoteFilePartitionReaderHelper(RemoteFileFormat.SCHEMA, partition) {
    private val results = scala.collection.mutable.Queue(downloadResults: _*)
    var clientClosed    = false

    override protected def downloadNextFile(): Boolean = {
      if (currentIterator != null && currentIterator.hasNext) { return true }
      if (results.isEmpty) { return false }
      processDownloadedFile("file.log", results.dequeue())
      currentIterator.hasNext
    }

    def triggerDownloadNextFile(): Boolean        = downloadNextFile()
    def getRecordsRead: Long                      = recordsRead
    def getTotalBytes: Long                       = totalBytesDownloaded
    def getFilesProcessed: Int                    = filesProcessed
    def getFilesFailed: Int                       = filesFailed
    def getFailedFiles: Seq[String]               = failedFiles
    def getCurrentRow: InternalRow                = currentRow
    def getCurrentIterator: Iterator[RawFileLine] = currentIterator

    override protected def closeClient(): Unit = clientClosed = true
  }

  describe("next()") {
    it("returns true and advances row for a successful download") {
      val line   = makeRawLine()
      val reader = new TestReader(List(DownloadSuccess(Iterator(line), DownloadMetrics(1, 100L))))
      reader.next() shouldBe true

      val result      = reader.get()
      val expectedRow = line.toInternalRow(RemoteFileFormat.SCHEMA)

      result shouldBe expectedRow
    }

    it("returns false when there are no files") {
      val reader = new TestReader(List.empty)
      reader.next() shouldBe false
    }

    it("tracks records and bytes across multiple files") {
      val line1 = makeRawLine()
      val line2 = makeRawLine()
      val reader = new TestReader(
        List(
          DownloadSuccess(Iterator(line1), DownloadMetrics(1, 50L)),
          DownloadSuccess(Iterator(line2), DownloadMetrics(1, 75L))
        )
      )
      reader.next() shouldBe true
      reader.next() shouldBe true
      reader.next() shouldBe false
      reader.getRecordsRead shouldBe 2
      reader.getTotalBytes shouldBe 125L
      reader.getFilesProcessed shouldBe 2
    }

    it("rethrows NonFatal exceptions") {
      val reader = new TestReader(Nil) {
        override protected def downloadNextFile(): Boolean = throw new Exception("testing failure")
      }
      an[Exception] should be thrownBy reader.next()
    }
  }

  describe("processDownloadedFile()") {
    it("tracks failed files on DownloadFailure") {
      val errorLine = makeRawLine(error = Some("HTTP 500"))
      val reader    = new TestReader(List(DownloadFailure(errorLine)))

      reader.next() // consume the error line
      reader.getFilesFailed shouldBe 1
      reader.getFailedFiles should contain("file.log")
    }
  }

  describe("close()") {
    it("calls closeClient and cleans up memory") {
      val line   = makeRawLine()
      val reader = new TestReader(List(DownloadSuccess(Iterator(line), DownloadMetrics(1, 10L))))
      reader.next()
      reader.close()
      reader.clientClosed shouldBe true
      reader.getCurrentRow shouldBe null
      reader.getCurrentIterator shouldBe null
    }

    it("calls closeClient, cleans up memory and log failed files") {
      val errorLine = makeRawLine(error = Some("HTTP 500"))
      val reader    = new TestReader(List(DownloadFailure(errorLine)))
      reader.next()

      reader.getFilesFailed shouldBe 1
      reader.getFailedFiles should contain("file.log")

      reader.close()

      reader.clientClosed shouldBe true
      reader.getCurrentRow shouldBe null
      reader.getCurrentIterator shouldBe null
      reader.getFailedFiles shouldBe empty
    }

    it("does not throw exception") {
      val reader = new TestReader(List.empty) {
        override protected def closeClient(): Unit = throw new Exception("close error")
      }
      noException should be thrownBy reader.close()
    }

    it("rethrows fatal exceptions from closeClient") {
      val reader = new TestReader(List.empty) {
        override protected def closeClient(): Unit = throw new StackOverflowError("fatal close error")
      }
      a[StackOverflowError] should be thrownBy reader.close()
    }

  }

  describe("currentMetricsValues()") {
    it("returns all expected metrics with correct names") {
      val reader = new TestReader(Nil)

      val resultMetrics = reader.currentMetricsValues()
      val metricNames   = resultMetrics.map(_.name()).toSet

      val expectedNames = Set(
        "avg files downloaded",
        "max files downloaded",
        "min files downloaded",
        "total bytes downloaded",
        "avg download duration",
        "max download duration",
        "min download duration",
        "avg records throughput",
        "max records throughput",
        "min records throughput",
        "avg bytes throughput",
        "max bytes throughput",
        "min bytes throughput"
      )

      metricNames shouldBe expectedNames
      resultMetrics should have size 13
    }

    it("computes metrics with correct values after processing files") {
      val line1 = makeRawLine()
      val line2 = makeRawLine()
      val reader = new TestReader(
        List(
          DownloadSuccess(Iterator(line1), DownloadMetrics(1, 50L)),
          DownloadSuccess(Iterator(line2), DownloadMetrics(1, 75L))
        )
      )
      reader.next()
      reader.next()

      val resultMetrics = reader.currentMetricsValues()
      val metricsByName = resultMetrics.map(m => m.name() -> m.value()).toMap

      metricsByName("avg files downloaded") shouldBe 2L
      metricsByName("max files downloaded") shouldBe 2L
      metricsByName("min files downloaded") shouldBe 2L
      metricsByName("total bytes downloaded") shouldBe 125L
    }
  }

  describe("cleanupMemory()") {
    it("clears byte arrays in unconsumed lines") {
      val bytes1 = Array[Byte](1, 2, 3)
      val bytes2 = Array[Byte](4, 5, 6)
      val line   = RawFileLine(remoteFile, downloadTimestamp = remoteFile.fetchedAt, logContent = Some(bytes1), rawFileBinary = Some(bytes2))
      val reader = new TestReader(List(DownloadSuccess(Iterator(line), DownloadMetrics(1, 0L))))
      // Trigger download but don't consume the line
      reader.triggerDownloadNextFile()
      reader.close()
      bytes1 shouldBe Array[Byte](0, 0, 0)
      bytes2 shouldBe Array[Byte](0, 0, 0)
    }
  }

  /**
   * Helper to create a RawFileLine with optional error for testing.
   * @param error optional error message to include in the line
   * @return a RawFileLine with the specified error and dummy byte arrays for content and raw binary
   */
  def makeRawLine(error: Option[String] = None): RawFileLine = {
    RawFileLine(remoteFile, downloadTimestamp = remoteFile.fetchedAt, error = error, logContent = Some(Array[Byte](1, 2, 3)), rawFileBinary = Some(Array[Byte](4, 5, 6)))
  }
}
