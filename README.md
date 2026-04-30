# Spark Remote File DataSource (`rest-file`)

A generic, extensible **Apache Spark DataSource V2** connector for reading remote files over HTTP/REST.  
It supports both **batch** and **streaming (micro-batch)** modes, and is designed to be pluggable: any remote endpoint can be supported by implementing a lightweight client interface.

---

## Repository Structure

This repository contains two logical modules in a single Maven project:

| Package | Visibility | Description |
|---|---|---|
| `com.amadeus.spark.datasource.remote.*` | **Open-source** | Generic framework: Spark DataSource V2 plumbing, client trait/registry, configuration, metrics |
| `com.imperva.spark.datasource.*` | **Private** | Imperva-specific implementation: `IncapsulaClient`, `ImpervaLogParser`, retry backend wrapper |

> **Open-sourcing:** Only the `com.amadeus.spark.datasource.remote` package is intended for public release.  
> The `com.imperva.spark.datasource` package contains proprietary Imperva integration code and must remain private.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Schema](#schema)
- [Configuration Options](#configuration-options)
- [Usage](#usage)
  - [Batch Mode](#batch-mode)
  - [Streaming Mode](#streaming-mode)
- [Implementing a Custom Client](#implementing-a-custom-client)
- [Metrics](#metrics)
- [Building](#building)
- [Testing](#testing)

---

## Overview

The `rest-file` DataSource reads files from a remote HTTP server that exposes:
1. A **file index endpoint** (`/logs.index`) that lists available files (one filename per line).
2. A **file download endpoint** that serves each individual file.

The framework handles parallelism, retries, streaming offsets, async prefetching, and Spark metrics automatically. Only the HTTP interaction details need to be provided by the client implementation.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│         com.amadeus.spark.datasource.remote  (open-source)   │
│  ┌────────────────┐   ┌─────────────────────────────────────┐│
│  │ RemoteFileBatch │   │     RemoteMicroBatchStream          ││
│  └───────┬────────┘   └──────────────┬──────────────────────┘│
│          │                           │                        │
│  ┌───────▼───────────────────────────▼──────────────────────┐│
│  │   RemoteFilePartitionReader / AsyncRemoteFilePartition-   ││
│  │                    Reader                                 ││
│  └──────────────────────────┬────────────────────────────────┘│
│                             │  RemoteFileClient (trait)       │
│                             │  ClientRegistry (ServiceLoader) │
└─────────────────────────────┼────────────────────────────────-┘
                              │
        ┌─────────────────────┴──────────────────────────┐
        │                                                │
┌───────▼───────────────────────┐   ┌───────────────────▼──────┐
│  com.imperva.spark.datasource  │   │  com.example.myclient    │
│  (private — Imperva-specific)  │   │  (your custom client)    │
│  · IncapsulaClient             │   │  · MyRemoteClient        │
│  · ImpervaLogParser            │   └──────────────────────────┘
│  · RetryingBackendWrapper      │
└────────────────────────────────┘
```

Key components (open-source):

| Component | Description |
|---|---|
| `RemoteFileTableProvider` | Entry point; registered as `rest-file` format |
| `RemoteFileBatch` | Batch read – plans partitions, distributes files round-robin |
| `RemoteMicroBatchStream` | Streaming read – tracks offsets, polls for new files |
| `RemoteFilePartitionReader` | Synchronous per-partition file download and row emission |
| `AsyncRemoteFilePartitionReader` | Asynchronous version with configurable prefetch queue |
| `ClientRegistry` | Discovers client implementations via Java `ServiceLoader` |
| `RemoteFileDataSourceOptions` | Typed configuration with validation and defaults |
| `RandomBytesClient` | Built-in diagnostic client generating synthetic data |

---

## Schema

Every row produced by this DataSource has the following schema:

```
root
 |-- sourceFile: struct
 |    |-- name: string          -- filename
 |    |-- fetchedAt: timestamp  -- when the file was discovered
 |    |-- fileSize: long        -- Content-Length from HTTP response
 |    |-- lastModified: timestamp
 |    |-- etag: string
 |    |-- requestId: string
 |-- logMetadata: string        -- JSON metadata (header before |==| separator)
 |-- logContent: binary         -- encrypted/raw content (after |==| separator)
 |-- rawFileBinary: binary      -- full file bytes when parsing fails
 |-- downloadTimestamp: timestamp
 |-- error: string              -- error message if download/parse failed
```

---

## Configuration Options

All options are passed via `.option(key, value)`.

### Required

| Option | Description |
|---|---|
| `uri` / `path` | Base URL of the remote server (e.g. `https://logs.example.com/api/account/12345`) |
| `remoteClient` | Short name or fully qualified class name of the client implementation |

### HTTP

| Option | Default | Description |
|---|---|---|
| `connectionTimeout` | `30s` | HTTP connection timeout. Supports `ms`, `s`, `m` suffixes |
| `readTimeout` | `60s` | HTTP read timeout |
| `maxRetries` | `3` | Maximum retry attempts for failed requests |
| `retryDelay` | `1s` | Delay between retries |
| `enableSsl` | `false` | Enable SSL/TLS |
| `trustStorePath` | — | Path to a custom trust store |
| `trustStorePassword` | — | Password for the trust store |

### Authentication

| Option | Default | Description |
|---|---|---|
| `apiKey` | — | API key (sent as HTTP Basic auth username) |
| `apiSecret` | — | API secret (sent as HTTP Basic auth password) |

### Parallelism

| Option | Default | Description |
|---|---|---|
| `partitions` | `4` | Number of Spark partitions for parallel download |

### Streaming

| Option | Default | Description |
|---|---|---|
| `pollInterval` | `2s` | How often to poll the file index for new files |
| `maxFilesPerTrigger` | `100` | Maximum files to process in a single micro-batch |
| `startOffset` | `earliest` | `earliest`, `latest`, or `file:<filename>` |

### Async Downloads

| Option | Default | Description |
|---|---|---|
| `asyncDownloads` | `false` | Enable async prefetch download pipeline |
| `asyncPrefetchSize` | `20` | Number of files to prefetch ahead |
| `asyncDownloadThreads` | `4` | Size of the async download thread pool |
| `asyncListFiles` | `false` | List files asynchronously in the background |

### Other

| Option | Default | Description |
|---|---|---|
| `sessionId` | — | Optional session identifier (useful for isolated test scenarios) |

---

## Usage

### Batch Mode

```scala
val df = spark.read
  .format("rest-file")
  .option("remoteClient", "imperva")          // client short name
  .option("uri", "https://logs.example.com/api/account/12345")
  .option("apiKey", "MY_API_KEY")
  .option("apiSecret", "MY_API_SECRET")
  .option("partitions", "8")
  .option("readTimeout", "30s")
  .option("enableSsl", "true")
  .load()

df.show()
```

### Streaming Mode

```scala
val stream = spark.readStream
  .format("rest-file")
  .option("remoteClient", "imperva")
  .option("uri", "https://logs.example.com/api/account/12345")
  .option("apiKey", "MY_API_KEY")
  .option("apiSecret", "MY_API_SECRET")
  .option("pollInterval", "10s")
  .option("maxFilesPerTrigger", "50")
  .option("startOffset", "latest")            // skip existing files
  .option("asyncDownloads", "true")           // enable prefetching
  .option("asyncPrefetchSize", "10")
  .load()

stream
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints/imperva")
  .start("/data/imperva-logs")
```

#### `startOffset` values

| Value | Behaviour |
|---|---|
| `earliest` | Process all files from the beginning |
| `latest` | Skip all existing files; only process new files arriving after the stream starts |
| `file:<filename>` | Start from (and including) the specified filename |

---

## Implementing a Custom Client

To support a new remote endpoint, implement the `RemoteFileClient` trait and register it via Java `ServiceLoader`.

### 1. Implement the trait

```scala
package com.example.myclient

import com.amadeus.spark.datasource.remote.client._
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions

class MyRemoteClient extends RemoteFileClient {

  private var options: RemoteFileDataSourceOptions = _

  // No-arg constructor required for ServiceLoader
  def this() = this(null)

  override def init(opts: RemoteFileDataSourceOptions): Unit = {
    this.options = opts
  }

  override def shortName(): String = "my-client"

  override def listLogFiles(): Seq[RemoteFile] = {
    // GET <uri>/logs.index and parse one filename per line
    ???
  }

  override def downloadLogFileWithResult(filename: String): DownloadResult = {
    // GET <uri>/<filename> and return DownloadSuccess or DownloadFailure
    ???
  }

  override def close(): Unit = {}
}
```

### 2. Register via ServiceLoader

Create the file:
```
src/main/resources/META-INF/services/com.amadeus.spark.datasource.remote.client.RemoteFileClientRegister
```

With content:
```
com.example.myclient.MyRemoteClient
```

### 3. Use it

```scala
spark.read
  .format("rest-file")
  .option("remoteClient", "my-client")   // matches shortName()
  .option("uri", "https://my-server.example.com")
  .load()
```

---

## Metrics

The connector exposes the following **custom Spark task metrics** visible in the Spark UI:

| Metric | Description |
|---|---|
| `filesDownloaded` | Number of files downloaded per task |
| `bytesDownloaded` | Total bytes downloaded per task |
| `downloadDurationMs` | Total download time in milliseconds |
| `bytesThroughput` | Bytes per second |
| `recordsThroughput` | Records per second |

Aggregated metrics (min/max/avg) are reported at the job level.

---

## Building

```bash
# Compile and run unit tests
mvn clean test

# Run integration tests (requires a running mock server)
mvn test -P integration-tests -Dtest=MockServerIntegrationTest

# Package (fat jar with shaded dependencies)
mvn clean package

# Package skipping tests
mvn clean package -DskipTests
```

**Requirements:**
- Java 11+
- Scala 2.12
- Apache Spark 3.5.x (provided scope — not bundled in the jar)
- Maven 3.6+

---

## Testing

Unit tests run without any external dependencies:

```bash
mvn test
```

Integration tests require the mock server to be running:

```bash
# Start the mock server (see imperva-specific README for details)
./start-mock-server.ps1

# Run integration tests
mvn test -P integration-tests
```

Test coverage is enforced via Scoverage (minimum threshold configured in `pom.xml`).

