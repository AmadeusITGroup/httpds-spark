package com.amadeus.spark.datasource.remote.streaming

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for RemoteFileOffset.
 */
class RemoteFileOffsetSpec extends AnyFunSpec with Matchers {

  describe("RemoteFileOffset") {

    it("should serialize and deserialize offset to/from JSON") {
      val files = Set("file1.log", "file2.log", "file3.log")
      val offset = RemoteFileOffset.fromFiles(files)

      val json = offset.json()
      val deserialized = RemoteFileOffset.fromJson(json)

      deserialized.indexFiles should equal(files)
    }

    it("should calculate files between two offsets") {
      val offset1 = RemoteFileOffset.fromFiles(Set("file1.log", "file2.log"))
      val offset2 = RemoteFileOffset.fromFiles(Set("file1.log", "file2.log", "file3.log", "file4.log"))

      val filesBetween = offset1.filesBetween(offset2)

      filesBetween should equal(Set("file3.log", "file4.log"))
    }

    it("should handle empty offset") {
      val emptyOffset = RemoteFileOffset.INITIAL

      emptyOffset.indexFiles shouldBe empty
      emptyOffset.json() should include("indexFiles")
    }

    it("should return empty set when no new files") {
      val offset1 = RemoteFileOffset.fromFiles(Set("file1.log", "file2.log"))
      val offset2 = RemoteFileOffset.fromFiles(Set("file1.log"))

      val filesBetween = offset1.filesBetween(offset2)

      filesBetween shouldBe empty
    }

    describe("offset equality") {

      it("should be equal when file lists are identical regardless of createdAt") {
        val files = Set("file1.log", "file2.log", "file3.log")
        val offset1 = RemoteFileOffset(files, 1000L)
        val offset2 = RemoteFileOffset(files, 2000L)

        offset1 should equal(offset2)
        offset1.hashCode() should equal(offset2.hashCode())
      }

      it("should not be equal when file lists differ") {
        val offset1 = RemoteFileOffset(Set("file1.log", "file2.log"), 1000L)
        val offset2 = RemoteFileOffset(Set("file1.log", "file3.log"), 1000L)

        offset1 should not equal offset2
      }

      it("should be equal for empty file lists regardless of createdAt") {
        val offset1 = RemoteFileOffset(Set.empty, 1000L)
        val offset2 = RemoteFileOffset(Set.empty, 5000L)

        offset1 should equal(offset2)
      }

      it("should maintain equality after serialization/deserialization") {
        val files = Set("file1.log", "file2.log")
        val offset1 = RemoteFileOffset(files, 1000L)
        val offset2 = RemoteFileOffset(files, 2000L)

        val deserialized1 = RemoteFileOffset.fromJson(offset1.json())
        val deserialized2 = RemoteFileOffset.fromJson(offset2.json())

        offset1 should equal(offset2)
        deserialized1 should equal(deserialized2)
        offset1 should equal(deserialized2)
        offset2 should equal(deserialized1)
      }
    }

    describe("serialization/deserialization") {

      it("should preserve all content through round-trip serialization") {
        val files = Set("file1.log", "file2.log", "file3.log", "file4.log")
        val createdAt = 1234567890L
        val offset = RemoteFileOffset(files, createdAt)

        val json = offset.json()
        val deserialized = RemoteFileOffset.fromJson(json)

        deserialized.indexFiles should equal(offset.indexFiles)
        deserialized.createdAt should equal(offset.createdAt)
      }

      it("should preserve empty set through round-trip serialization") {
        val offset = RemoteFileOffset(Set.empty, 9876543210L)

        val json = offset.json()
        val deserialized = RemoteFileOffset.fromJson(json)

        deserialized.indexFiles shouldBe empty
        deserialized.createdAt should equal(offset.createdAt)
      }

      it("should preserve large file sets through round-trip serialization") {
        val files = (1 to 1000).map(i => s"file$i.log").toSet
        val offset = RemoteFileOffset(files, System.currentTimeMillis())

        val json = offset.json()
        val deserialized = RemoteFileOffset.fromJson(json)

        deserialized.indexFiles should equal(offset.indexFiles)
        deserialized.createdAt should equal(offset.createdAt)
      }

      it("should preserve files with special characters through serialization") {
        val files = Set(
          "file-with-dashes.log",
          "file_with_underscores.log",
          "file.with.dots.log",
          "file@with#special$chars.log",
          "file with spaces.log"
        )
        val offset = RemoteFileOffset(files, 1000L)

        val json = offset.json()
        val deserialized = RemoteFileOffset.fromJson(json)

        deserialized.indexFiles should equal(offset.indexFiles)
        deserialized.createdAt should equal(offset.createdAt)
      }
    }

    describe("filesBetween with functionally equivalent offsets") {

      it("should give same result when start offset is deserialized") {
        val startFiles = Set("file1.log", "file2.log")
        val endFiles = Set("file1.log", "file2.log", "file3.log", "file4.log")

        val startOffset = RemoteFileOffset.fromFiles(startFiles)
        val endOffset = RemoteFileOffset.fromFiles(endFiles)

        // Deserialize start offset
        val startDeserialized = RemoteFileOffset.fromJson(startOffset.json())

        val result1 = startOffset.filesBetween(endOffset)
        val result2 = startDeserialized.filesBetween(endOffset)

        result1 should equal(result2)
        result1 should equal(Set("file3.log", "file4.log"))
      }

      it("should give same result when end offset is deserialized") {
        val startFiles = Set("file1.log", "file2.log")
        val endFiles = Set("file1.log", "file2.log", "file3.log", "file4.log")

        val startOffset = RemoteFileOffset.fromFiles(startFiles)
        val endOffset = RemoteFileOffset.fromFiles(endFiles)

        // Deserialize end offset
        val endDeserialized = RemoteFileOffset.fromJson(endOffset.json())

        val result1 = startOffset.filesBetween(endOffset)
        val result2 = startOffset.filesBetween(endDeserialized)

        result1 should equal(result2)
        result1 should equal(Set("file3.log", "file4.log"))
      }

      it("should give same result when both offsets are deserialized") {
        val startFiles = Set("file1.log", "file2.log")
        val endFiles = Set("file1.log", "file2.log", "file3.log", "file4.log", "file5.log")

        val startOffset = RemoteFileOffset.fromFiles(startFiles)
        val endOffset = RemoteFileOffset.fromFiles(endFiles)

        // Deserialize both offsets
        val startDeserialized = RemoteFileOffset.fromJson(startOffset.json())
        val endDeserialized = RemoteFileOffset.fromJson(endOffset.json())

        val result1 = startOffset.filesBetween(endOffset)
        val result2 = startDeserialized.filesBetween(endDeserialized)

        result1 should equal(result2)
        result1 should equal(Set("file3.log", "file4.log", "file5.log"))
      }

      it("should give same result with different createdAt timestamps") {
        val files1 = Set("file1.log", "file2.log")
        val files2 = Set("file1.log", "file2.log", "file3.log")

        val startOffset1 = RemoteFileOffset(files1, 1000L)
        val startOffset2 = RemoteFileOffset(files1, 2000L)
        val endOffset1 = RemoteFileOffset(files2, 3000L)
        val endOffset2 = RemoteFileOffset(files2, 4000L)

        val result1 = startOffset1.filesBetween(endOffset1)
        val result2 = startOffset2.filesBetween(endOffset2)

        result1 should equal(result2)
        result1 should equal(Set("file3.log"))
      }

      it("should give same result for empty start offset") {
        val endFiles = Set("file1.log", "file2.log", "file3.log")

        val startOffset1 = RemoteFileOffset.INITIAL
        val startOffset2 = RemoteFileOffset.fromFiles(Set.empty)
        val endOffset = RemoteFileOffset.fromFiles(endFiles)

        val result1 = startOffset1.filesBetween(endOffset)
        val result2 = startOffset2.filesBetween(endOffset)

        result1 should equal(result2)
        result1 should equal(endFiles)
      }

      it("should give same result for empty end offset") {
        val startFiles = Set("file1.log", "file2.log")

        val startOffset = RemoteFileOffset.fromFiles(startFiles)
        val endOffset1 = RemoteFileOffset.INITIAL
        val endOffset2 = RemoteFileOffset.fromFiles(Set.empty)

        val result1 = startOffset.filesBetween(endOffset1)
        val result2 = startOffset.filesBetween(endOffset2)

        result1 should equal(result2)
        result1 shouldBe empty
      }

      it("should give same result with multiple serialization/deserialization cycles") {
        val startFiles = Set("file1.log", "file2.log")
        val endFiles = Set("file1.log", "file2.log", "file3.log", "file4.log")

        val startOffset = RemoteFileOffset.fromFiles(startFiles)
        val endOffset = RemoteFileOffset.fromFiles(endFiles)

        // Multiple serialization cycles
        val startCycle1 = RemoteFileOffset.fromJson(startOffset.json())
        val startCycle2 = RemoteFileOffset.fromJson(startCycle1.json())
        val startCycle3 = RemoteFileOffset.fromJson(startCycle2.json())

        val endCycle1 = RemoteFileOffset.fromJson(endOffset.json())
        val endCycle2 = RemoteFileOffset.fromJson(endCycle1.json())

        val result1 = startOffset.filesBetween(endOffset)
        val result2 = startCycle3.filesBetween(endCycle2)

        result1 should equal(result2)
        result1 should equal(Set("file3.log", "file4.log"))
      }
    }
  }
}
