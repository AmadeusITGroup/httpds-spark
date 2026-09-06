package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.client.RemoteFile
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.concurrent.{Future, TimeoutException}

class RemoteDiscoveryFailureTest extends AnyFunSpec with Matchers {
  private def stream(mode: String, failure: String): RemoteMicroBatchStream = {
    val options = new CaseInsensitiveStringMap(
      Map(
        "uri"            -> "http://localhost:5000",
        "remoteClient"   -> classOf[RecoveringDiscoveryClient].getName,
        "asyncListFiles" -> "true",
        "startOffset"    -> (if (mode == "latest") "latest" else "earliest"),
        "failure"        -> failure
      ).asJava
    )
    val result = new RemoteMicroBatchStream(new StructType(), options)
    if (mode == "availableNow") result.prepareForTriggerAvailableNow()
    result
  }

  for (mode <- Seq("latest", "availableNow"); failure <- Seq("network", "timeout", "synchronous")) {
    it(s"should propagate $failure discovery failure in $mode mode and allow a fresh discovery") {
      val source = stream(mode, failure)
      def discover(): RemoteFileOffset = {
        val offset = if (mode == "latest") source.initialOffset() else source.latestOffset(RemoteFileOffset.INITIAL, ReadLimit.allAvailable())
        RemoteFileOffset.convert(offset)
      }

      try {
        val error = intercept[Exception](discover())
        error.getMessage shouldBe failure
        if (failure == "timeout") error shouldBe a[TimeoutException]

        // A failed call must not freeze an empty initial offset or AvailableNow snapshot.
        discover().indexFiles shouldBe Set("existing.log")
        discover().indexFiles shouldBe Set("existing.log")
      } finally source.stop()
    }
  }

  for (mode <- Seq("latest", "availableNow")) {
    it(s"should still accept a successful empty listing in $mode mode") {
      val source = stream(mode, "empty")
      try {
        if (mode == "latest") RemoteFileOffset.convert(source.initialOffset()).indexFiles shouldBe empty
        else Option(source.latestOffset(RemoteFileOffset.INITIAL, ReadLimit.allAvailable())) shouldBe empty
      } finally source.stop()
    }
  }
}

/** Per-client state keeps discovery failure tests isolated without global fixtures. */
class RecoveringDiscoveryClient(config: RemoteFileDataSourceOptions) extends StubAsyncClient(Seq.empty) {
  private val attempts = new AtomicInteger()

  override def listLogFilesAsync(): Future[Seq[RemoteFile]] = {
    val failure = config.allOptions.get("failure")
    if (failure == "empty") Future.successful(Seq.empty)
    else if (attempts.getAndIncrement() == 0) {
      failure match {
        case "timeout"     => Future.failed(new TimeoutException(failure))
        case "synchronous" => throw new IllegalStateException(failure)
        case _             => Future.failed(new IllegalStateException(failure))
      }
    } else Future.successful(Seq(RemoteFile("existing.log", Timestamp.valueOf("2026-09-06 12:00:00"))))
  }
}
