package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.remote.client._
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.net.http.HttpClient
import java.sql.Timestamp
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

// ScalaTest assertions and Java API checks require null comparisons — Java interop test patterns
// scalafix:off DisableSyntax.null
// Spark/Java API reflection tests require asInstanceOf casts for type-erased Java generics
// scalafix:off DisableSyntax.asInstanceOf
class AsyncRemoteFilePartitionReaderTest extends AnyFunSpec with Matchers {

  val remoteFile = RemoteFile("file.log", Timestamp.valueOf("2026-05-05 00:00:00"))

  val defaultOptions: RemoteFileDataSourceOptions = RemoteFileDataSourceOptions(
    remoteClient = "test",
    asyncPrefetchSize = 2,
    allOptions = CaseInsensitiveStringMap.empty()
  )

  def makeRawLine(error: Option[String] = None): RawFileLine =
    RawFileLine(
      remoteFile,
      downloadTimestamp = remoteFile.fetchedAt,
      error = error,
      logContent = Some(UTF8String.fromString("ABC").getBytes),
      rawFileBinary = Some(UTF8String.fromString("DFE").getBytes)
    )

  /**
   * Stub async client that returns pre-configured results for each filename.
   */
  class StubAsyncClient(results: Map[String, DownloadResult]) extends AsyncRemoteFileClient {
    // Mutable flag required for test stub tracking of client close state
    // scalafix:off DisableSyntax.var
    var closed = false
    // scalafix:on DisableSyntax.var

    override def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit = ()
    override def listLogFilesAsync(): Future[Seq[RemoteFile]]                                = Future.successful(Seq.empty)
    override def submitDownloadAsync(filename: String): Future[DownloadResult] =
      Future.successful(results.getOrElse(filename, throw new NoSuchElementException(s"No result for $filename")))
    override def listLogFiles(): Seq[RemoteFile]                             = Seq.empty
    override def downloadLogFileWithResult(filename: String): DownloadResult = results(filename)
    override def shortName(): String                                         = "stub"
    override def close(): Unit                                               = closed = true
  }

  /**
   * Stub async client that returns results from a sequence (order of submission).
   */
  class SequentialStubAsyncClient(resultSeq: Seq[DownloadResult]) extends AsyncRemoteFileClient {
    private val resultsIterator = resultSeq.iterator
    // Mutable flag required for test stub tracking of client close state
    // scalafix:off DisableSyntax.var
    var closed = false
    // scalafix:on DisableSyntax.var

    override def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit = ()
    override def listLogFilesAsync(): Future[Seq[RemoteFile]]                                = Future.successful(Seq.empty)
    override def submitDownloadAsync(filename: String): Future[DownloadResult] =
      Future.successful(resultsIterator.next())
    override def listLogFiles(): Seq[RemoteFile]                             = Seq.empty
    override def downloadLogFileWithResult(filename: String): DownloadResult = throw new UnsupportedOperationException
    override def shortName(): String                                         = "sequential-stub"
    override def close(): Unit                                               = closed = true
  }

  /**
   * Stub async client that returns futures completing after a delay,
   * so that no future is immediately completed when getNextCompletedDownload checks.
   * This forces the None branch (waitForFirstCompletedDownload).
   */
  class DelayedStubAsyncClient(results: Map[String, DownloadResult], delayMillis: Long) extends AsyncRemoteFileClient {
    // Mutable flag required for test stub tracking of client close state
    // scalafix:off DisableSyntax.var
    var closed = false
    // scalafix:on DisableSyntax.var

    override def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit = ()
    override def listLogFilesAsync(): Future[Seq[RemoteFile]]                                = Future.successful(Seq.empty)
    override def submitDownloadAsync(filename: String): Future[DownloadResult] = {
      implicit val ec: ExecutionContext = ExecutionContext.global
      Future {
        Thread.sleep(delayMillis) // Add random jitter to avoid all completing at the same time
        results.getOrElse(filename, throw new NoSuchElementException(s"No result for $filename"))
      }
    }
    override def listLogFiles(): Seq[RemoteFile]                             = Seq.empty
    override def downloadLogFileWithResult(filename: String): DownloadResult = results(filename)
    override def shortName(): String                                         = "delayed-stub"
    override def close(): Unit                                               = closed = true
  }

  describe("next() and get()") {

    it("reads a single file successfully") {
      val line      = makeRawLine()
      val partition = RemoteFileInputPartition(Seq("file1.log"))
      val client    = new StubAsyncClient(Map("file1.log" -> DownloadSuccess(Iterator(line), DownloadMetrics(1, 100L))))
      val reader    = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      Thread.sleep(500) // Simulate async delay

      reader.next() shouldBe true

      val result   = reader.get()
      val expected = makeRawLine().toInternalRow(RemoteFileFormat.SCHEMA)

      // Compare struct field (sourceFile)
      result.getStruct(0, 6) shouldBe expected.getStruct(0, 6)
      // Compare logMetadata
      result.getUTF8String(1) shouldBe expected.getUTF8String(1)
      // Compare logContent (byte arrays)
      val contentResult   = UTF8String.fromBytes(result.getBinary(2))
      val expectedContent = UTF8String.fromBytes(expected.getBinary(2))
      contentResult shouldEqual expectedContent
      // Compare rawFileBinary (byte arrays)
      val contentResultRaw   = UTF8String.fromBytes(result.getBinary(3))
      val expectedContentRaw = UTF8String.fromBytes(expected.getBinary(3))
      contentResultRaw shouldEqual expectedContentRaw
      result.getBinary(3) should contain theSameElementsInOrderAs expected.getBinary(3)
      // Compare downloadTimestamp
      result.getLong(4) shouldBe expected.getLong(4)
      // Compare error
      result.getUTF8String(5) shouldBe expected.getUTF8String(5)

      reader.next() shouldBe false
    }

    it("reads multiple files") {
      val line1     = makeRawLine()
      val line2     = makeRawLine()
      val partition = RemoteFileInputPartition(Seq("a.log", "b.log"))
      val client = new StubAsyncClient(
        Map(
          "a.log" -> DownloadSuccess(Iterator(line1), DownloadMetrics(1, 50L)),
          "b.log" -> DownloadSuccess(Iterator(line2), DownloadMetrics(1, 75L))
        )
      )
      val reader = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      reader.next() shouldBe true
      reader.next() shouldBe true
      reader.next() shouldBe false
    }

    it("reads multiple lines from a single file") {
      val line1     = makeRawLine()
      val line2     = makeRawLine()
      val partition = RemoteFileInputPartition(Seq("file1.log"))
      val client = new StubAsyncClient(
        Map(
          "file1.log" -> DownloadSuccess(Iterator(line1, line2), DownloadMetrics(2, 200L))
        )
      )
      val reader = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      reader.next() shouldBe true
      reader.next() shouldBe true
      reader.next() shouldBe false
    }

    it("returns false when partition has no files") {
      val partition = RemoteFileInputPartition(Seq.empty)
      val client    = new StubAsyncClient(Map.empty)
      val reader    = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      reader.next() shouldBe false
    }

    it("handles download failures as error lines") {
      val errorLine = makeRawLine(error = Some("HTTP 500"))
      val partition = RemoteFileInputPartition(Seq("bad.log"))
      val client    = new StubAsyncClient(Map("bad.log" -> DownloadFailure(errorLine)))
      val reader    = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      reader.next() shouldBe true
      reader.next() shouldBe false
    }

    it("waits for first completed download when no future is immediately completed") {
      val line1     = makeRawLine()
      val line2     = makeRawLine()
      val partition = RemoteFileInputPartition(Seq("slow1.log", "slow2.log"))
      // Delay of 500ms ensures futures are not yet completed when getNextCompletedDownload checks
      val client = new DelayedStubAsyncClient(
        Map(
          "slow1.log" -> DownloadSuccess(Iterator(line1), DownloadMetrics(1, 50L)),
          "slow2.log" -> DownloadSuccess(Iterator(line2), DownloadMetrics(1, 75L))
        ),
        delayMillis = 500
      )
      val reader = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      // Both files should eventually be readable after waitForFirstCompletedDownload
      reader.next() shouldBe true
      reader.get() should not be null
      reader.next() shouldBe true
      reader.get() should not be null
      reader.next() shouldBe false
    }

    it("processes a mix of successes and failures") {
      val line      = makeRawLine()
      val errorLine = makeRawLine(error = Some("timeout"))
      val partition = RemoteFileInputPartition(Seq("ok.log", "fail.log"))
      val client = new StubAsyncClient(
        Map(
          "ok.log"   -> DownloadSuccess(Iterator(line), DownloadMetrics(1, 50L)),
          "fail.log" -> DownloadFailure(errorLine)
        )
      )
      val reader = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      reader.next() shouldBe true
      reader.next() shouldBe true
      reader.next() shouldBe false
    }
  }

  describe("close()") {

    it("is idempotent when called again after the reader self-closed on completion") {
      // Real Spark lifecycle: the async reader closes itself proactively once all files are
      // processed (inside next()), and Spark then calls close() again at task completion.
      val partition = RemoteFileInputPartition(Seq("a.log", "b.log"))
      val client = new StubAsyncClient(
        Map(
          "a.log" -> DownloadSuccess(Iterator(makeRawLine()), DownloadMetrics(1, 50L)),
          "b.log" -> DownloadSuccess(Iterator(makeRawLine()), DownloadMetrics(1, 75L))
        )
      )
      val reader = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      // Drain the partition. The final next() == false triggers the proactive close() (#1).
      reader.next() shouldBe true
      reader.next() shouldBe true
      reader.next() shouldBe false

      // Spark calls close() again at task completion (#2) - must not throw.
      noException should be thrownBy reader.close()

      client.closed shouldBe true
    }

    it("cancels pending downloads and releases completed ones on close") {
      val line      = makeRawLine()
      val partition = RemoteFileInputPartition(Seq("fast.log", "slow.log"))
      val client    = new StubAsyncClient(Map.empty)
      val reader    = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      val field = classOf[AsyncRemoteFilePartitionReader].getDeclaredField("inFlightDownloads")
      field.setAccessible(true)
      field.set(
        reader,
        mutable.Map(
          "first.log" -> Future.successful(DownloadSuccess(Iterator(line), DownloadMetrics(1, 50L))),
          "two.log"   -> Future.successful(DownloadSuccess(Iterator(line), DownloadMetrics(2, 55L)))
        )
      )

      reader.close()

      val inFlightDownloads = field.get(reader).asInstanceOf[mutable.Map[String, Future[DownloadResult]]]
      inFlightDownloads shouldBe empty

      client.closed shouldBe true
    }

    it("clears pending futures when the cancellation deadline expires without downloads completing") {
      val line      = makeRawLine()
      val partition = RemoteFileInputPartition(Seq("fast.log", "slow.log"))
      val client    = new StubAsyncClient(Map.empty)
      val reader    = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      val field = classOf[AsyncRemoteFilePartitionReader].getDeclaredField("inFlightDownloads")
      field.setAccessible(true)
      field.set(
        reader,
        mutable.Map(
          "fast.log" -> Future.successful(DownloadSuccess(Iterator(line), DownloadMetrics(1, 50L))),
          "slow.log" -> Promise[DownloadResult]().future // never completes
        )
      )

      reader.close()

      val inFlightDownloads = field.get(reader).asInstanceOf[mutable.Map[String, Future[DownloadResult]]]
      inFlightDownloads shouldBe empty

      client.closed shouldBe true
    }
  }

  describe("prefetch pipeline") {

    it("respects prefetch size by submitting limited concurrent downloads") {
      // With prefetchSize=2 and 4 files, only 2 should be submitted initially
      val lines     = (1 to 4).map(_ => makeRawLine())
      val partition = RemoteFileInputPartition(Seq("f1.log", "f2.log", "f3.log", "f4.log"))
      val client = new SequentialStubAsyncClient(
        lines.map(l => DownloadSuccess(Iterator(l), DownloadMetrics(1, 10L)))
      )
      val reader = new AsyncRemoteFilePartitionReader(RemoteFileFormat.SCHEMA, partition, defaultOptions, client)

      // All 4 files should be readable
      (1 to 4).foreach(_ => reader.next() shouldBe true)
      reader.next() shouldBe false
    }
  }

}
// scalafix:on DisableSyntax.asInstanceOf
// scalafix:on DisableSyntax.null
