package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.client.{AsyncRemoteFileClient, DownloadResult, RemoteFile}
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.net.http.HttpClient
import java.sql.Timestamp
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Unit tests for WorkloadRefresherAsyncHelper trait.
 */
class WorkloadRefresherAsyncHelperTest extends AnyFunSpec with Matchers {

  // --- Test mocks ---

  /** A failing async client to test error handling */
  class FailingAsyncClient extends StubAsyncClient(Seq.empty) {
    override def listLogFilesAsync(): Future[Seq[RemoteFile]] =
      Future.failed(new RuntimeException("network error"))
  }

  /** Concrete implementation of the trait under test */
  class TestHelper(initialFiles: Seq[RemoteFile] = Seq.empty) extends WorkloadRefresherAsyncHelper {
    // Mutable state required for tracking test helper file list and merge call counts
    // scalafix:off DisableSyntax.var
    private var _files: Seq[RemoteFile]  = initialFiles
    var mergeCallCount: Int              = 0
    var lastMergedFiles: Seq[RemoteFile] = Seq.empty
    // scalafix:on DisableSyntax.var

    override protected def discoveredFiles: Seq[RemoteFile] = _files

    def currentFiles: Seq[RemoteFile] = _files

    override protected def mergeFiles(indexFiles: Seq[RemoteFile]): Unit = {
      mergeCallCount += 1
      lastMergedFiles = indexFiles
      _files = _files ++ indexFiles
    }

    // Expose protected methods for testing
    def testInitAsyncClient(config: RemoteFileDataSourceOptions, client: AsyncRemoteFileClient): Unit = initAsyncClient(config, client)

    def testFetchFilesAsyncFromServer(client: AsyncRemoteFileClient): Seq[RemoteFile] = fetchFilesAsyncFromServer(client)

    def testRefreshAsyncFileList(client: AsyncRemoteFileClient): Unit = refreshAsyncFileList(client)

    def testWaitForPendingAsyncOperation(): Unit = waitForPendingAsyncOperation()
  }

  // --- Helpers ---
  def defaultConfig: RemoteFileDataSourceOptions =
    RemoteFileDataSourceOptions.fromMap(
      new CaseInsensitiveStringMap(Map("uri" -> "http://localhost:5000", "remoteClient" -> "stub").asJava)
    )

  // --- Tests ---

  describe("initAsyncClient") {
    it("should call initAsync on the async client") {
      val helper = new TestHelper()
      val client = new StubAsyncClient(Seq.empty)

      helper.testInitAsyncClient(defaultConfig, client)

      client.initAsyncCalled shouldBe true
    }
  }

  describe("fetchFilesAsyncFromServer") {
    it("should return files fetched from the async client") {
      val files = Seq(
        RemoteFile("file1.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file2.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )
      val client = new StubAsyncClient(files)
      val helper = new TestHelper()

      val result = helper.testFetchFilesAsyncFromServer(client)

      val expected = Seq(
        RemoteFile("file1.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file2.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )

      result shouldEqual expected
    }

    it("should return an empty sequence when the async client fails") {
      val client = new FailingAsyncClient
      val helper = new TestHelper()

      val result = helper.testFetchFilesAsyncFromServer(client)

      result shouldBe empty
    }
  }

  describe("refreshAsyncFileList") {
    it("should call mergeFiles with the fetched files") {
      val existingFile = RemoteFile("existing.log", Timestamp.valueOf("2026-05-12 07:00:32"))
      val files = Seq(
        RemoteFile("file1.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file2.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )
      val client = new StubAsyncClient(files)
      val helper = new TestHelper(initialFiles = Seq(existingFile))

      helper.testRefreshAsyncFileList(client)
      Thread.sleep(500) // Wait for async merge to complete

      val expected = Seq(
        RemoteFile("existing.log", Timestamp.valueOf("2026-05-12 07:00:32")),
        RemoteFile("file1.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file2.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )

      helper.mergeCallCount shouldBe 1
      helper.lastMergedFiles shouldEqual files
      helper.currentFiles shouldEqual expected
    }

    it("should wait synchronously when discoveredFiles is initially empty") {
      val files = Seq(
        RemoteFile("file3.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file4.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )
      val client = new StubAsyncClient(files)
      val helper = new TestHelper(initialFiles = Seq.empty)

      // After the call returns, files must already be populated (blocking wait triggered)
      helper.testRefreshAsyncFileList(client)
      Thread.sleep(500) // Wait for async merge to complete

      val expected = Seq(
        RemoteFile("file3.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file4.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )

      helper.currentFiles shouldEqual expected
    }

    it("should not throw when the async client fails") {
      val client = new FailingAsyncClient
      val helper = new TestHelper()

      noException should be thrownBy helper.testRefreshAsyncFileList(client)
    }
  }

  describe("waitForPendingAsyncOperation") {
    it("should complete without error when there is no pending operation") {
      val helper = new TestHelper()

      noException should be thrownBy helper.testWaitForPendingAsyncOperation()
    }

    it("should wait for background merge to finish before proceeding") {
      val existingFile = RemoteFile("existing.log", Timestamp.valueOf("2026-05-12 07:00:32"))
      val files = Seq(
        RemoteFile("file1.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file2.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )
      val client = new StubAsyncClient(files)
      val helper = new TestHelper(initialFiles = Seq(existingFile))

      helper.testRefreshAsyncFileList(client)
      helper.testWaitForPendingAsyncOperation()

      val expected = Seq(
        RemoteFile("existing.log", Timestamp.valueOf("2026-05-12 07:00:32")),
        RemoteFile("file1.log", Timestamp.valueOf("2026-05-12 08:00:32")),
        RemoteFile("file2.log", Timestamp.valueOf("2026-05-12 08:00:30"))
      )

      helper.currentFiles shouldEqual expected
    }
  }
}

/** A minimal AsyncRemoteFileClient stub */
class StubAsyncClient(filesToReturn: Seq[RemoteFile]) extends AsyncRemoteFileClient {
  // Mutable flag required for test stub tracking of async init call
  // scalafix:off DisableSyntax.var
  var initAsyncCalled = false
  // scalafix:on DisableSyntax.var

  override def initAsync(executionContext: ExecutionContext, httpClient: HttpClient): Unit =
    initAsyncCalled = true

  override def listLogFilesAsync(): Future[Seq[RemoteFile]] = {
    Future.successful(filesToReturn)
  }

  override def submitDownloadAsync(filename: String): Future[DownloadResult] =
    Future.failed(new UnsupportedOperationException("not used in tests"))

  // RemoteFileClient stubs (unused)
  override def shortName(): String = "stub"
  override def init(config: RemoteFileDataSourceOptions): Unit = {}
  override def listLogFiles(): Seq[RemoteFile]                             = filesToReturn
  override def downloadLogFileWithResult(filename: String): DownloadResult = throw new UnsupportedOperationException("not used in tests")
  override def close(): Unit = {}
}
