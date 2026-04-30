package com.amadeus.spark.datasource.remote.conf

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for StartOffsets.
 */
class StartOffsetsTest extends AnyFunSpec with Matchers {

  describe("StartOffsets.isValid") {

    it("should return EARLIEST for 'earliest'") {
      StartOffsets.isValid("earliest") shouldBe StartOffsets.EARLIEST
    }

    it("should return EARLIEST for 'EARLIEST' (case-insensitive)") {
      StartOffsets.isValid("EARLIEST") shouldBe StartOffsets.EARLIEST
    }

    it("should return LATEST for 'latest'") {
      StartOffsets.isValid("latest") shouldBe StartOffsets.LATEST
    }

    it("should return LATEST for 'LATEST' (case-insensitive)") {
      StartOffsets.isValid("LATEST") shouldBe StartOffsets.LATEST
    }

    it("should return FILENAME for 'file:<filename>'") {
      StartOffsets.isValid("file:myfile.log") shouldBe StartOffsets.FILENAME
    }

    it("should return FILENAME for 'file:' with any filename") {
      StartOffsets.isValid("file:some/path/to/file.txt") shouldBe StartOffsets.FILENAME
    }

    it("should throw IllegalArgumentException for an unknown option") {
      an[IllegalArgumentException] should be thrownBy StartOffsets.isValid("unknown")
    }

    it("should throw IllegalArgumentException for an empty string") {
      an[IllegalArgumentException] should be thrownBy StartOffsets.isValid("")
    }

    it("should throw IllegalArgumentException for 'file' without colon prefix") {
      an[IllegalArgumentException] should be thrownBy StartOffsets.isValid("file")
    }
  }
}

