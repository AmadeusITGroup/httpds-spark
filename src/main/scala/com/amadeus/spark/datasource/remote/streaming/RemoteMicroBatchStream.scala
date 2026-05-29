package com.amadeus.spark.datasource.remote.streaming

import com.amadeus.spark.datasource.remote.RemoteFilePartitionReaderFactory
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions
import com.amadeus.spark.datasource.remote.helpers.PartitionsHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Optional

/**
 * Micro-batch stream implementation for remote file data source.
 *
 * This class implements the MicroBatchStream interface to enable structured streaming
 * with micro-batches. It also implements SupportsAdmissionControl to provide dynamic
 * rate limiting based on maxFilesPerTrigger configuration, and SupportsTriggerAvailableNow
 * to support Spark's AvailableNow trigger mode for processing all available data at query start.
 */
class RemoteMicroBatchStream(schema: StructType, options: CaseInsensitiveStringMap)
    extends MicroBatchStream
    with SupportsAdmissionControl
    with SupportsTriggerAvailableNow
    with ReportsSourceMetrics
    with PartitionsHelper
    with Logging {

  private lazy val workloadManager = new WorkloadRefresher(config)

  // Cache for "latest" mode initial offset
  @volatile private var cachedLatestModeInitialOffset: Option[RemoteFileOffset] = None

  // AvailableNow trigger state
  @volatile private var isTriggerAvailableNow: Boolean        = false
  @volatile private var availableNowFileSnapshot: Set[String] = _

  // Validate streaming configuration
  private val config: RemoteFileDataSourceOptions = {
    val configurationOptions = RemoteFileDataSourceOptions.fromMap(options)
    configurationOptions.validate()
    configurationOptions
  }

  /**
   * Returns the initial offset for this stream.
   *
   * This is called when no checkpoint exists.
   *
   * Spark may call this method multiple times during query planning and optimization.
   * For "latest" and "file:" modes, we MUST cache the initial offset to ensure consistency.
   *
   * Behavior depends on startOffset configuration:
   * - "earliest": Returns INITIAL offset (empty set) to process all available files from the beginning
   * - "latest": Returns offset with all currently available files to skip existing data and start from new files.
   *             The offset is cached on first computation to ensure subsequent calls return the same value.
   * - "file:<filename>": Returns offset with all files that alphabetically precede the specified filename,
   *                      effectively starting from that filename onwards. The offset is cached on first computation.
   */
  override def initialOffset(): Offset = {

    config.startOffset.toLowerCase match {
      case "earliest"                                                       => RemoteFileOffset.INITIAL
      case "latest"                                                         => cacheOffset()
      case RemoteFileDataSourceOptions.filenameOffsetPattern(startFilename) => cacheOffset(f => f < startFilename)
      case _                                                                =>
        // This should never happen due to validation in RemoteFileDataSourceOptions
        logWarning(s"[STREAM-$instanceId]   Invalid startOffset '${config.startOffset}', defaulting to 'earliest'")
        RemoteFileOffset.INITIAL
    }

  }

  /**
   * Caches the initial offset for "latest" mode or "file:" mode.
   *
   * This method computes the initial offset based on the current files in the workload manager,
   * applying an optional filter function to determine which files to include in the offset.
   *
   * The computed offset is cached in a volatile variable to ensure that subsequent calls to initialOffset()
   * return the same value, providing consistency during query planning and execution.
   *
   * @param filterFiles an optional function to filter which files should be included in the offset
   * @return the computed and cached initial offset
   */
  private def cacheOffset(filterFiles: String => Boolean = _ => true) = {
    cachedLatestModeInitialOffset match {
      case Some(cached) => cached
      case None =>
        val currentFiles = workloadManager.getAllCurrentFiles.filter(filterFiles)
        val offset       = RemoteFileOffset.fromFiles(currentFiles)

        cachedLatestModeInitialOffset = Some(offset)

        logInfo(s"[STREAM-$instanceId]   Cached initial offset for 'latest' mode with ${currentFiles.size} existing files to skip")

        offset
    }
  }

  /**
   * Returns the latest available offset.
   *
   * This is called to determine what new data is available since the last batch.
   * It should poll the data source for new files and return an offset representing
   * all available data.
   */
  override def latestOffset(): Offset = latestOffset(RemoteFileOffset.INITIAL, ReadLimit.allAvailable())

  /**
   * Returns the latest available offset with a read limit.
   *
   * This method is called by Spark to determine what new data is available since the last batch,
   * while also applying a read limit (e.g., maxFilesPerTrigger) for admission control.
   *
   * @param startOffset the starting offset containing already processed files
   * @param readLimit the read limit for admission control (e.g., maxFilesPerTrigger)
   * @return the latest offset representing new data to process, or null if no new data is available
   */
  override def latestOffset(startOffset: Offset, readLimit: ReadLimit): Offset = {
    val workload = retrieveFiles(readLimit, startOffset)

    if (workload.filesToProcess.isEmpty) {
      return null
    }

    val newOffset = RemoteFileOffset.fromFiles(workload.fullFileList)
    logDebug(s"[STREAM-$instanceId]   Returning new offset: $newOffset")

    newOffset
  }

  /**
   * Retrieves the list of files to process based on the starting offset and read limit.
   *
   * This method handles both normal continuous mode and AvailableNow trigger mode.
   * In AvailableNow mode, it uses a cached snapshot of available files to ensure consistent processing.
   *
   * @param readLimit the read limit for admission control (e.g., maxFilesPerTrigger)
   * @param startOffset the starting offset containing already processed files
   * @return a Workload object containing the files to process in this batch and the full file list for the new offset
   */
  private[streaming] def retrieveFiles(readLimit: ReadLimit, startOffset: Offset): Workload = {
    val restFileStartOffset = RemoteFileOffset.convert(startOffset)
    val maxFiles            = computeMaxFiles(readLimit)

    if (isTriggerAvailableNow) {
      latestOffsetForAvailableNow(restFileStartOffset, maxFiles)
    } else {
      // Continuous mode - discover files fresh each time
      workloadManager.refreshWorkload(restFileStartOffset.indexFiles, maxFiles)
    }

  }

  /**
   * Extracts the maxFiles limit from the ReadLimit object.
   *
   * @param readLimit the ReadLimit object provided by Spark
   * @return the maxFiles limit to apply for this batch
   */
  private[streaming] def computeMaxFiles(readLimit: ReadLimit): Int = {
    readLimit match {
      case readMaxFiles: ReadMaxFiles =>
        logDebug(s"[STREAM-$instanceId]   Extracted maxFiles from ReadLimit: ${readMaxFiles.maxFiles()}")
        readMaxFiles.maxFiles()
      case _: ReadAllAvailable =>
        logDebug(s"[STREAM-$instanceId]   ReadLimit is ReadAllAvailable, setting maxFiles to Int.MaxValue")
        Int.MaxValue
      case _ =>
        logWarning(s"[STREAM-$instanceId]   ReadLimit is not ReadMaxFiles type (${readLimit.getClass.getSimpleName}), using config default ${config.maxFilesPerTrigger}")
        config.maxFilesPerTrigger
    }
  }

  /**
   * Handles latestOffset logic for AvailableNow trigger mode.
   *
   * On first call: Discovers and caches ALL available files (snapshot isolation).
   * On subsequent calls: Uses cached snapshot, applies rate limit, returns files to process.
   *
   * @param startOffset the starting offset containing already processed files
   * @param maxFiles maximum number of files to process in this batch
   * @return tuple of (filesToProcess, fullFileList)
   */
  private def latestOffsetForAvailableNow(startOffset: RemoteFileOffset, maxFiles: Int): Workload = {
    val processedFiles = startOffset.indexFiles

    // First call in AvailableNow mode - cache ALL available files
    if (Option(availableNowFileSnapshot).isEmpty) {
      logDebug(s"[STREAM-$instanceId]   AvailableNow: First latestOffset call - discovering and caching all files")
      availableNowFileSnapshot = workloadManager.discoverAllFiles()
    }

    // Calculate new files from cached snapshot (not yet processed)
    val newFiles = availableNowFileSnapshot.diff(processedFiles)

    if (newFiles.isEmpty) {
      return Workload(Seq.empty, processedFiles)
    }

    // Apply rate limit to new files (sorted for deterministic ordering)
    val filesToProcess = newFiles.toSeq.sorted.take(maxFiles)
    val fullFileList   = processedFiles ++ filesToProcess.toSet

    logDebug(s"[STREAM-$instanceId]   AvailableNow: ${newFiles.size} files remaining, processing ${filesToProcess.size} this batch")

    Workload(filesToProcess, fullFileList)
  }

  /**
   * Returns the read limit for admission control.
   */
  override def getDefaultReadLimit: ReadLimit = ReadLimit.maxFiles(config.maxFilesPerTrigger)

  /**
   * Called ONCE by Spark at query start when using Trigger.AvailableNow().
   * Sets internal flag to enable snapshot caching behavior.
   *
   * When this method is called, the stream will:
   * 1. Discover and cache ALL available files on the first latestOffset() call
   * 2. Process files from the cached snapshot, respecting rate limits (maxFilesPerTrigger)
   * 3. Return null from latestOffset() when all cached files are processed, causing Spark to stop
   * 4. Ignore any new files that arrive after the snapshot is taken
   */
  override def prepareForTriggerAvailableNow(): Unit = {
    logInfo(s"[STREAM-$instanceId] ══════════════════════════════════════════════════════════════")
    logInfo(s"[STREAM-$instanceId] >>> prepareForTriggerAvailableNow() called")
    logInfo(s"[STREAM-$instanceId]   Trigger Mode: AVAILABLE_NOW activated")
    logInfo(s"[STREAM-$instanceId]   Behavior: Will cache all available files on first latestOffset() call")
    logInfo(s"[STREAM-$instanceId]   Rate Limit: maxFilesPerTrigger=${config.maxFilesPerTrigger} will apply to EVERY batch")
    logInfo(s"[STREAM-$instanceId] ══════════════════════════════════════════════════════════════")
    logDebug(s"[STREAM-$instanceId]   Thread: ${Thread.currentThread().getName}")

    isTriggerAvailableNow = true
  }

  /**
   * Deserializes an offset from JSON.
   */
  override def deserializeOffset(json: String): Offset = RemoteFileOffset.fromJson(json)

  /**
   * Plans input partitions for a micro-batch.
   *
   * This is called after latestOffset() to determine how to parallelize
   * reading the new data.
   *
   * NOTE: Spark's Catalyst optimizer may call this method multiple times during
   * query planning and optimization phases before actual execution. This is NORMAL.
   *
   * @param start the starting offset (exclusive)
   * @param end   the ending offset (inclusive)
   * @return array of input partitions
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset: RemoteFileOffset = start
    val endOffset: RemoteFileOffset   = end

    // Get files to process in this batch
    val filesToProcess = startOffset.filesBetween(endOffset)

    if (filesToProcess.isEmpty) {
      return Array.empty
    }

    // Create one partition per file for parallel downloading
    // We can also group files into partitions if there are too many small files
    val numPartitions = math.min(filesToProcess.size, config.numPartitions)
    val partitions    = createPartitions(numPartitions, filesToProcess.toSeq)

    partitions.asInstanceOf[Array[InputPartition]]
  }

  /**
   * Creates a reader factory for the partitions.
   */
  override def createReaderFactory(): PartitionReaderFactory = new RemoteFilePartitionReaderFactory(schema, options)

  /**
   * Commits the given offset.
   *
   * This is called after a micro-batch completes successfully.
   * Update the internal state to reflect committed progress.
   *
   * @param end the offset that has been committed
   */
  override def commit(end: Offset): Unit = {

    val endOffset: RemoteFileOffset = end

    logDebug(s"[STREAM-$instanceId] ═══════════════════════════════════════════════════════════")
    logDebug(s"[STREAM-$instanceId]   Committing offset: $end")
    logDebug(s"[STREAM-$instanceId]   ✓ Committed with ${endOffset.indexFiles.size} processed files")
    logDebug(s"[STREAM-$instanceId]   Files: ${endOffset.indexFiles.mkString(", ")}")
    logDebug(s"[STREAM-$instanceId] ═══════════════════════════════════════════════════════════")

  }

  /**
   * Stops the stream and releases resources.
   */
  override def stop(): Unit = {

    if (isTriggerAvailableNow) {
      // Reset AvailableNow state
      logDebug(s"[STREAM-$instanceId]   Clearing AvailableNow cached snapshot (${if (availableNowFileSnapshot != null) availableNowFileSnapshot.size else 0} files)")

      availableNowFileSnapshot = null
      isTriggerAvailableNow = false
    }

    workloadManager.stop()
  }

  /**
   * Reports source metrics for monitoring.
   *
   * @param latestConsumedOffset the latest offset that has been consumed
   * @return a map of metric names to values
   */
  override def metrics(latestConsumedOffset: Optional[Offset]): util.Map[String, String] = {
    val metrics = new util.HashMap[String, String]()

    if (latestConsumedOffset.isPresent) {
      val offset                             = latestConsumedOffset.get()
      val remoteFileOffset: RemoteFileOffset = offset

      metrics.put("latestConsumedOffsetFileCount", remoteFileOffset.indexFiles.size.toString)
      metrics.put("latestConsumedOffsetCreatedAt", remoteFileOffset.createdAt.toString)
      metrics.put("nbDiscoveredFiles", workloadManager.nbDiscoveredFiles.toString)
    }

    metrics
  }
}
