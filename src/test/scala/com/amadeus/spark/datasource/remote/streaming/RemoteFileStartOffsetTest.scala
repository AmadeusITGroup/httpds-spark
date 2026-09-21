package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.client.RemoteFile
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.collection.JavaConverters._

class RemoteFileStartOffsetTest extends AnyFunSpec with Matchers {
  private def stream(startOffset: String): RemoteMicroBatchStream = {
    val options = new CaseInsensitiveStringMap(
      Map(
        "uri"          -> "http://localhost:5000",
        "remoteClient" -> classOf[MixedCaseFilesClient].getName,
        "startOffset"  -> startOffset
      ).asJava
    )
    new RemoteMicroBatchStream(new StructType(), options)
  }

  for ((filename, skipped) <- Seq("B.log" -> Set("A.log"), "b.log" -> Set("A.log", "B.log"))) {
    it(s"preserves the case of file:$filename and includes the requested file") {
      val source = stream(s"file:$filename")
      try {
        val initial = RemoteFileOffset.convert(source.initialOffset())
        initial.indexFiles shouldBe skipped
        source.initialOffset() shouldBe theSameInstanceAs(initial)
        val workload = source.retrieveFiles(ReadLimit.allAvailable(), initial)
        workload.filesToProcess.toSet shouldBe (Set("A.log", "B.log", "b.log", "c.log") -- skipped)
        workload.filesToProcess should contain(filename)
      } finally source.stop()
    }
  }

  for ((mode, skipped) <- Seq("EARLIEST" -> Set.empty[String], "LaTeSt" -> Set("A.log", "B.log", "b.log", "c.log"))) {
    it(s"still accepts case-insensitive $mode mode") {
      val source = stream(mode)
      try RemoteFileOffset.convert(source.initialOffset()).indexFiles shouldBe skipped
      finally source.stop()
    }
  }
}

class MixedCaseFilesClient
    extends StubAsyncClient(
      Seq("A.log", "B.log", "b.log", "c.log").map(name => RemoteFile(name, new Timestamp(0L)))
    )
