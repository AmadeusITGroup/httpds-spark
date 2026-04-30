package com.amadeus.spark.datasource.remote.conf

/**
 * Enumeration for specifying the starting offsets when reading from a remote file source.
 * This is used to determine where to begin processing files in a streaming context.
 *
 * The options include:
 * - EARLIEST: Start processing from the earliest available files (default).
 * - LATEST: Start processing from the latest available files, ignoring existing ones.
 *
 * This enumeration can be extended in the future to include additional offset strategies
 * such as specific timestamps or file patterns.
 */
object StartOffsets extends Enumeration {
  type StartOffsets = Value

  val EARLIEST: Value = Value("earliest")

  val LATEST: Value = Value("latest")

  val FILENAME: Value = Value("file")

  /**
   * Parses a string option to determine the corresponding StartOffsets value.
   *
   * Valid options are:
   * - "earliest": corresponds to StartOffsets.EARLIEST
   * - "latest": corresponds to StartOffsets.LATEST
   * - "file:<filename>": corresponds to StartOffsets.FILENAME with the specified filename
   *
   * @param offsetOption the string option to parse
   * @return the corresponding StartOffsets value
   * @throws IllegalArgumentException if the option is invalid
   */
  def isValid(offsetOption: String): StartOffsets = {
    offsetOption.toLowerCase match {
      case "earliest"                                             => EARLIEST
      case "latest"                                               => LATEST
      case _ if offsetOption.startsWith(s"${FILENAME.toString}:") => FILENAME
      case other                                                  => throw new IllegalArgumentException(s"Invalid start offset option: '$other'. Valid options are: 'earliest', 'latest', 'file:<filename>'.")
    }
  }
}
