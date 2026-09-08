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
- [Maintainers](#maintainers)
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
| sbt          | 1.12+   |

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

Options are validated before batch or streaming execution. Partition, trigger-file, prefetch, and download-thread counts must be positive; retries must be non-negative. Durations must be finite: `connectTimeout` must be at least 1ms and `readTimeout` positive; `pollInterval` and `retryDelay` may be zero for immediate polling or retrying.

### Required

| Option         | Description                                                             |
|----------------|-------------------------------------------------------------------------|
| `uri` / `path` | Base URL of the remote server (e.g. `https://files.example.com/api/v1`) |
| `remoteClient` | Short name or fully qualified class name of the client implementation   |

### HTTP

These options are parsed and passed to the selected client; validation does not guarantee that the client implements them. The framework configures `connectTimeout` on its driver/executor async HTTP clients, but does not add authentication, retries, or custom trust stores. Consult your client's documentation for supported options.

| Option               | Default | Description                                                 |
|----------------------|---------|-------------------------------------------------------------|
| `connectTimeout`     | `30s`   | HTTP connection timeout (`ms`, `s`, `m` suffixes supported); `connectionTimeout` is an alias |
| `readTimeout`        | `60s`   | Requested HTTP read timeout; the client must apply it to requests |
| `maxRetries`         | `3`     | Requested maximum retries; the client must implement retry behavior |
| `retryDelay`         | `1s`    | Requested delay between retries; client-managed             |
| `enableSsl`          | `false` | Client-specific TLS option; does not toggle TLS in the framework |
| `trustStorePath`     | —       | Client-specific trust-store path; supported formats depend on the client |
| `trustStorePassword` | —       | Client-specific trust-store password                       |

The async partition reader also uses twice `readTimeout` when waiting for a download to complete. This is a reader wait limit, not an HTTP request timeout or a guarantee of request cancellation; async discovery has separate fixed wait limits.

Framework-created HTTP clients use JVM-default TLS configuration and the request URI scheme. `enableSsl` does not rewrite `http://` to `https://`, and `trustStorePath` / `trustStorePassword` do not configure these shared clients. A client requiring custom trust must explicitly support an appropriate TLS configuration; do not assume that setting these options is sufficient.

### Authentication

| Option      | Default | Description                                   |
|-------------|---------|-----------------------------------------------|
| `apiKey`    | —       | Credential passed to the client; authentication scheme is client-defined |
| `apiSecret` | —       | Credential passed to the client; authentication scheme is client-defined |

The framework does not send these credentials or install an HTTP authenticator. Basic auth, bearer tokens, custom headers, and credential handling are responsibilities of the selected client.

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

Async executor resources are shared by tasks with the same `asyncDownloadThreads` and `connectTimeout` values. Different settings use separate pools, retained until executor shutdown. Avoid generating many distinct configurations in a long-lived executor.

Legacy options `spark.remoteFile.asyncDownload.threads` and `spark.remoteFile.connectionTimeout` (milliseconds) remain accepted. Canonical options take precedence; for timeouts, precedence is `connectTimeout`, then `connectionTimeout`, then the legacy option. The defaults are 4 threads and 30 seconds for all readers.

### Other

| Option      | Default | Description                                             |
|-------------|---------|---------------------------------------------------------|
| `sessionId` | —       | Optional session identifier (useful for test isolation) |

## Usage

### Batch Mode

The authentication and TLS options below are illustrative and require support from `my-client`.

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

This example requires `my-client` to implement authentication and `AsyncRemoteFileClient`.

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

Client implementations own HTTP request construction, authentication, request timeouts, retries/backoff, and response parsing. Document which options they honor and reject unsupported security settings rather than silently ignoring them. Async clients receive framework-managed resources with the configuration described above; receiving those resources does not implement request-level policies automatically.

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

## Maintainers

- Simone DE SANTIS ([@sdeswork](https://github.com/sdeswork))
- Guillaume LECLERC ([@guleclerc](https://github.com/guleclerc))

See [.github/CODEOWNERS](.github/CODEOWNERS) for review ownership, and [SECURITY.md](SECURITY.md) to report vulnerabilities.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
