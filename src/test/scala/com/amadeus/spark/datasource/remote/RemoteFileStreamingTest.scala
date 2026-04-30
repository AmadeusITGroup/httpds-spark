package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.SparkTestBase
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

/**
 * E2E test for streaming reads with the remote file data source.
 */
class RemoteFileStreamingTest extends AnyFunSpec with Matchers with SparkTestBase {

  describe("RemoteFile Streaming Read") {

    it("should process micro-batch with mocked client") {
      val sparkSession = spark
      import sparkSession.implicits._

      // Create a temporary checkpoint directory
      val checkpointDir = Files.createTempDirectory("checkpoint").toString

      try {
        // Create streaming DataFrame
        val streamDf = spark.readStream
          .format("rest-file")
          .option("uri", "http://localhost:5000")
          .option("remoteClient", "mock")
          .option("maxFilesPerTrigger", "2")
          .load()

        // Start a streaming query that writes to memory
        val query = streamDf.writeStream
          .format("memory")
          .queryName("test_query")
          .trigger(Trigger.AvailableNow())
          .option("checkpointLocation", checkpointDir)
          .start()

        // Wait for the query to process
        query.processAllAvailable()

        // Read from the memory sink
        val resultDf = spark.sql("SELECT * FROM test_query")
        val result   = resultDf.as[RemoteFileRow].collect()

        val fetchTime = Timestamp.valueOf(LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES))

        val expected = Seq(
          RemoteFileRow(
            sourceFile = SourceFile("file1.log", fetchTime),
            logContent = Some("""{"timestamp":"2024-01-01T10:00:00Z","message":"Log entry 1"}""".getBytes),
            downloadTimestamp = fetchTime
          ),
          RemoteFileRow(
            sourceFile = SourceFile("file2.log", fetchTime),
            logContent = Some("""{"timestamp":"2024-01-01T10:01:00Z","message":"Log entry 2"}""".getBytes),
            downloadTimestamp = fetchTime
          ),
          RemoteFileRow(
            sourceFile = SourceFile("file3.log", fetchTime),
            logContent = Some("""{"timestamp":"2024-01-01T10:02:00Z","message":"Log entry 3"}""".getBytes),
            downloadTimestamp = fetchTime
          )
        )

        result.length shouldEqual expected.length
        result zip expected foreach { case (actual, exp) =>
          actual.sourceFile.copy(fetchedAt = Timestamp.valueOf(actual.sourceFile.fetchedAt.toLocalDateTime.truncatedTo(ChronoUnit.MINUTES))) shouldEqual exp.sourceFile
          actual.logContent.map(UTF8String.fromBytes).orNull shouldEqual exp.logContent.map(UTF8String.fromBytes).orNull
          actual.rawFileBinary.map(UTF8String.fromBytes).orNull shouldEqual exp.rawFileBinary.map(UTF8String.fromBytes).orNull
          actual.error shouldEqual exp.error
        }

        // Stop the query
        query.stop()
      } finally {
        // Clean up checkpoint directory
        val checkpointPath = new java.io.File(checkpointDir)
        if (checkpointPath.exists()) {
          deleteRecursively(checkpointPath)
        }
      }
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
