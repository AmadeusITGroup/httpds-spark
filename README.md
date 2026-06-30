# httpds-spark

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala&logoColor=white)](https://www.scala-lang.org/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

A generic, extensible **Apache Spark DataSource V2** connector for reading remote files over HTTP/REST.
It supports both **batch** and **streaming (micro-batch)** modes, and is designed to be pluggable — any HTTP endpoint can be supported by implementing a lightweight client interface.

## Features

- **Batch & Streaming** — read remote files in batch or as a continuous micro-batch stream
- **Pluggable clients** — implement a single trait to add support for any HTTP-based file source
- **Async prefetching** — optional asynchronous download pipeline for higher throughput
- **Built-in metrics** — custom Spark task metrics (files downloaded, bytes, throughput) visible in the Spark UI
- **Offset tracking** — streaming mode tracks offsets and supports `earliest`, `latest`, or file-based start positions
- **ServiceLoader discovery** — client implementations are registered via standard Java `ServiceLoader`

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Batch Mode](#batch-mode)
  - [Streaming Mode](#streaming-mode)
- [Output Schema](#output-schema)
- [Implementing a Custom Client](#implementing-a-custom-client)
- [Architecture](#architecture)
- [Metrics](#metrics)
- [Building & Testing](#building--testing)
- [Contributing](#contributing)
- [License](#license)

## Quick Start

```scala
val df = spark.read
  .format("httpds")
  .option("remoteClient", "my-client")
  .option("uri", "https://files.example.com/api/v1")
  .option("partitions", "8")
  .load()

df.show()
```

## Installation

### Prerequisites

| Requirement  | Version |
|--------------|---------|
| Java         | 11+     |
| Scala        | 2.12    |
| Apache Spark | 3.5.x   |
| sbt          | 1.10+   |

### Build the JAR

```bash
sbt package
```

The resulting JAR is located in `target/scala-2.12/`. Add it to your Spark application's classpath:

```bash
spark-submit --jars httpds-spark_2.12-1.0.0-SNAPSHOT.jar ...
```

### sbt dependency (local publish)

```bash
sbt publishLocal
```

Then add to your `build.sbt`:

```scala
libraryDependencies += "com.amadeus.spark" %% "httpds-spark" % "1.0.0-SNAPSHOT"
```

> **Note:** Spark and SLF4J are declared as `Provided` dependencies — they are expected to be available at runtime in your Spark cluster.

## Configuration

All options are passed via `.option(key, value)` on the DataFrameReader/StreamReader.

### Required

| Option         | Description                                                             |
|----------------|-------------------------------------------------------------------------|
| `uri` / `path` | Base URL of the remote server (e.g. `https://files.example.com/api/v1`) |
| `remoteClient` | Short name or fully qualified class name of the client implementation   |

### HTTP

| Option               | Default | Description                                                 |
|----------------------|---------|-------------------------------------------------------------|
| `connectionTimeout`  | `30s`   | HTTP connection timeout (`ms`, `s`, `m` suffixes supported) |
| `readTimeout`        | `60s`   | HTTP read timeout                                           |
| `maxRetries`         | `3`     | Maximum retry attempts for failed requests                  |
| `retryDelay`         | `1s`    | Delay between retries                                       |
| `enableSsl`          | `false` | Enable SSL/TLS                                              |
| `trustStorePath`     | —       | Path to a custom trust store (JKS/PEM)                      |
| `trustStorePassword` | —       | Password for the trust store                                |

### Authentication

| Option      | Default | Description                                   |
|-------------|---------|-----------------------------------------------|
| `apiKey`    | —       | API key (sent as HTTP Basic auth username)    |
| `apiSecret` | —       | API secret (sent as HTTP Basic auth password) |

### Parallelism

| Option       | Default | Description                                      |
|--------------|---------|--------------------------------------------------|
| `partitions` | `4`     | Number of Spark partitions for parallel download |

### Streaming

| Option               | Default    | Description                                      |
|----------------------|------------|--------------------------------------------------|
| `pollInterval`       | `2s`       | How often to poll the file index for new files   |
| `maxFilesPerTrigger` | `100`      | Maximum files to process in a single micro-batch |
| `startOffset`        | `earliest` | `earliest`, `latest`, or `file:<filename>`       |

### Async Downloads

| Option                 | Default | Description                                 |
|------------------------|---------|---------------------------------------------|
| `asyncDownloads`       | `false` | Enable async prefetch download pipeline     |
| `asyncPrefetchSize`    | `20`    | Number of files to prefetch ahead           |
| `asyncDownloadThreads` | `4`     | Size of the async download thread pool      |
| `asyncListFiles`       | `false` | List files asynchronously in the background |

### Other

| Option      | Default | Description                                             |
|-------------|---------|---------------------------------------------------------|
| `sessionId` | —       | Optional session identifier (useful for test isolation) |

## Usage

### Batch Mode

```scala
val df = spark.read
  .format("httpds")
  .option("remoteClient", "my-client")
  .option("uri", "https://files.example.com/api/v1")
  .option("apiKey", sys.env("API_KEY"))
  .option("apiSecret", sys.env("API_SECRET"))
  .option("partitions", "8")
  .option("readTimeout", "30s")
  .option("enableSsl", "true")
  .load()

df.printSchema()
df.show()
```

### Streaming Mode

```scala
val stream = spark.readStream
  .format("httpds")
  .option("remoteClient", "my-client")
  .option("uri", "https://files.example.com/api/v1")
  .option("apiKey", sys.env("API_KEY"))
  .option("apiSecret", sys.env("API_SECRET"))
  .option("pollInterval", "10s")
  .option("maxFilesPerTrigger", "50")
  .option("startOffset", "latest")
  .option("asyncDownloads", "true")
  .option("asyncPrefetchSize", "10")
  .load()

stream.writeStream
  .format("parquet")
  .option("checkpointLocation", "/checkpoints/my-source")
  .start("/data/my-source")
```

#### Start offset values

| Value | Behaviour |
|---|---|
| `earliest` | Process all files from the beginning |
| `latest` | Skip existing files; only process new arrivals |
| `file:<filename>` | Start from (and including) the specified filename |

## Output Schema

Every row produced by this DataSource follows this schema:

```
root
 |-- sourceFile: struct
 |    |-- name: string            — filename
 |    |-- fetchedAt: timestamp    — when the file was discovered
 |    |-- fileSize: long          — Content-Length from HTTP response
 |    |-- lastModified: timestamp — Last-Modified HTTP header
 |    |-- etag: string            — ETag HTTP header
 |    |-- requestId: string       — Request ID HTTP header
 |-- logMetadata: string          — parsed metadata from file content (if any)
 |-- logContent: binary           — file content ready for processing
 |-- rawFileBinary: binary        — full raw file bytes (fallback when parsing fails)
 |-- downloadTimestamp: timestamp  — when the file was downloaded
 |-- error: string                — error message if download/parsing failed
```

## Implementing a Custom Client

To integrate a new HTTP-based file source, implement the `RemoteFileClient` trait and register it via Java `ServiceLoader`.

### 1. Implement the trait

```scala
package com.example.myclient

import com.amadeus.spark.datasource.remote.client._
import com.amadeus.spark.datasource.remote.conf.RemoteFileDataSourceOptions

class MyRemoteClient extends RemoteFileClient {

  private var options: RemoteFileDataSourceOptions = _

  override def init(opts: RemoteFileDataSourceOptions): Unit =
    this.options = opts

  override def shortName(): String = "my-client"

  override def listLogFiles(): Seq[RemoteFile] = {
    // Call your HTTP endpoint and return the list of available files
    ???
  }

  override def downloadLogFileWithResult(filename: String): DownloadResult = {
    // Download the file and return DownloadSuccess or DownloadFailure
    ???
  }

  override def close(): Unit = {
    // Release any resources (HTTP client, etc.)
  }
}
```

> **Tip:** Optionally implement `AsyncRemoteFileClient` to enable the async prefetch pipeline, or `FileSorter` to control file ordering.

### 2. Register via ServiceLoader

Create the file:

```
src/main/resources/META-INF/services/com.amadeus.spark.datasource.remote.client.RemoteFileClientRegister
```

With the fully qualified class name:

```
com.example.myclient.MyRemoteClient
```

### 3. Use it

```scala
spark.read
  .format("httpds")
  .option("remoteClient", "my-client")   // matches shortName()
  .option("uri", "https://my-server.example.com")
  .load()
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│           com.amadeus.spark.datasource.remote  (framework)       │
│                                                                  │
│  ┌────────────────────┐     ┌──────────────────────────────────┐ │
│  │  RemoteFileBatch   │     │  RemoteMicroBatchStream          │ │
│  │  (batch read)      │     │  (streaming read)                │ │
│  └────────┬───────────┘     └───────────────┬──────────────────┘ │
│           │                                 │                    │
│  ┌────────▼─────────────────────────────────▼──────────────────┐ │
│  │  RemoteFilePartitionReader / AsyncRemoteFilePartitionReader │ │
│  └────────────────────────────┬────────────────────────────────┘ │
│                               │                                  │
│              RemoteFileClient (trait) ◄── ClientRegistry         │
│                     (ServiceLoader discovery)                    │
└───────────────────────────────┼──────────────────────────────────┘
                                │
                                │
                   ┌────────────▼─────────┐
                   │  Your custom client: │
                   │  MyRemoteClient      │
                   └──────────────────────┘
```

### Key Components

| Component                        | Description                                               |
|----------------------------------|-----------------------------------------------------------|
| `RemoteFileTableProvider`        | Entry point; registered as the `httpds` format         |
| `RemoteFileBatch`                | Plans partitions and distributes files round-robin        |
| `RemoteMicroBatchStream`         | Tracks offsets and polls for new files                    |
| `RemoteFilePartitionReader`      | Synchronous per-partition file download and row emission  |
| `AsyncRemoteFilePartitionReader` | Asynchronous variant with configurable prefetch queue     |
| `ClientRegistry`                 | Discovers client implementations via Java `ServiceLoader` |
| `RemoteFileDataSourceOptions`    | Typed configuration with validation and defaults          |

## Metrics

The connector exposes custom **Spark task metrics** visible in the Spark UI:

| Metric               | Description                         |
|----------------------|-------------------------------------|
| `filesDownloaded`    | Number of files downloaded per task |
| `bytesDownloaded`    | Total bytes downloaded per task     |
| `downloadDurationMs` | Total download time in milliseconds |
| `bytesThroughput`    | Bytes per second                    |
| `recordsThroughput`  | Records per second                  |

Aggregated metrics (min / max / avg) are reported at the job level.

## Building & Testing

### Build

```bash
sbt compile    # Compile the project
sbt package    # Package the JAR
```

### Test

```bash
sbt test       # Run all unit tests
```

Tests use [ScalaTest](https://www.scalatest.org/) and [WireMock](https://wiremock.org/) — no external services required.

### Dependencies

| Library      | Version          |
|--------------|------------------|
| Apache Spark | 3.5.0 (provided) |
| Scala        | 2.12.18          |
| sttp client4 | 4.0.13           |
| ScalaTest    | 3.2.17           |
| WireMock     | 3.13.2           |

## Contributing

Contributions are welcome! Please read the [Contributing Guide](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) before submitting a pull request.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
