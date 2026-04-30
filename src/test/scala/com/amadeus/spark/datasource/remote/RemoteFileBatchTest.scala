package com.amadeus.spark.datasource.remote

import com.amadeus.spark.datasource.SparkTestBase
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

/**
 * E2E test for batch reads with the remote file data source.
 */
class RemoteFileBatchTest extends AnyFunSpec with Matchers with SparkTestBase {

  describe("Custom Metrics") {

    it("should report max files downloaded metric") {
      val sparkSession = spark
      import sparkSession.implicits._

      // Create DataFrame using the data source with mock client
      val result = spark.read
        .format("rest-file")
        .option("uri", "http://localhost:5000")
        .option("remoteClient", "mock")
        .option("numPartitions", "2") // Use 2 partitions to distribute files
        .load()
        .as[RemoteFileRow]
        .collect()

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
    }
  }
}
