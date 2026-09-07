package com.amadeus.spark.datasource.remote.read

import com.amadeus.spark.datasource.remote.RemoteFileFormat
import com.amadeus.spark.datasource.remote.client.{DownloadResult, RemoteFile, RemoteFileClient}
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.JavaConverters._

class BatchClientLifecycleTest extends AnyFunSpec with Matchers {
  private def withBatch(mode: String, failClose: Boolean = false)(check: (RemoteFileBatch, BatchListingState) => Assertion): Assertion = {
    val sessionId = UUID.randomUUID().toString
    val state     = new BatchListingState()
    BatchListingState.sessions.put(sessionId, state)
    val options = new CaseInsensitiveStringMap(
      Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> classOf[BatchListingClient].getName,
        "sessionId"    -> sessionId,
        "mode"         -> mode,
        "failClose"    -> failClose.toString
      ).asJava
    )
    try check(new RemoteFileBatch(RemoteFileFormat.SCHEMA, options), state)
    finally { val _ = BatchListingState.sessions.remove(sessionId) }
  }

  for (mode <- Seq("success", "empty", "lazy")) {
    it(s"closes a $mode listing client and creates a fresh client when replanning") {
      withBatch(mode) { (batch, state) =>
        val expectedPartitions = if (mode == "empty") 0 else 2
        batch.planInputPartitions().length shouldBe expectedPartitions
        state.created.get() shouldBe 1
        state.closed.get() shouldBe 1
        batch.planInputPartitions().length shouldBe expectedPartitions
        state.created.get() shouldBe 2
        state.closed.get() shouldBe 2
      }
    }
  }

  for (mode <- Seq("listing-failure", "sorting-failure"); failClose <- Seq(false, true)) {
    it(s"closes after $mode and preserves the original error when failClose=$failClose") {
      withBatch(mode, failClose) { (batch, state) =>
        intercept[IllegalStateException](batch.planInputPartitions()) shouldBe theSameInstanceAs(state.listingFailure)
        state.created.get() shouldBe 1
        state.closed.get() shouldBe 1
      }
    }
  }

  it("retains successful planning results when close fails") {
    withBatch("success", failClose = true) { (batch, state) =>
      batch.planInputPartitions() should have length 2
      state.closed.get() shouldBe 1
    }
  }
}

class BatchListingState {
  val created        = new AtomicInteger()
  val closed         = new AtomicInteger()
  val listingFailure = new IllegalStateException("listing failed")
}

object BatchListingState {
  val sessions = new ConcurrentHashMap[String, BatchListingState]()
}

class BatchListingClient(config: RemoteFileDataSourceOptions) extends RemoteFileClient {
  private val state        = BatchListingState.sessions.get(config.sessionId.get)
  private val closed       = new AtomicBoolean()
  private val mode         = config.allOptions.get("mode")
  private val registration = state.created.incrementAndGet()

  override def shortName(): String = s"batch-listing-$registration"

  override def listLogFiles(): Seq[RemoteFile] = {
    require(!closed.get(), "client already closed")
    if (mode == "listing-failure") throw state.listingFailure
    else if (mode == "empty") Seq.empty
    else Seq("A.log", "B.log").map(name => RemoteFile(name, new Timestamp(0L)))
  }

  override def listAndSortLogFiles(): Seq[RemoteFile] = {
    if (mode == "sorting-failure") throw state.listingFailure
    else if (mode == "lazy") listLogFiles().toStream.map { file =>
      require(!closed.get(), "lazy listing consumed after close")
      file
    }
    else super.listAndSortLogFiles()
  }

  override def downloadLogFileWithResult(filename: String): DownloadResult = throw new UnsupportedOperationException(filename)

  override def close(): Unit = {
    closed.set(true)
    state.closed.incrementAndGet()
    if (config.allOptions.getBoolean("failClose", false)) throw new IllegalStateException("close failed")
  }
}
