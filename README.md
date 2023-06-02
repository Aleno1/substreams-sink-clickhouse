# Substreams Sink Clickhouse

This is a command line tool to quickly sync a Substreams to a Clickhouse database.

## Project Status

- :construction: Work in progress

### Quickstart

1. Install `substreams-sink-clickhouse` by using the pre-built binary release [available in the releases page](https://github.com/Aleno1/substreams-sink-clickhouse/releases). Extract `substreams-sink-clickhouse` binary into a folder and ensure this folder is referenced globally via your `PATH` environment variable.

    > **Note** Or install from source directly `go install github.com/Aleno1/substreams-sink-clickhouse/cmd/substreams-sink-clickhouse@latest`.

1. Have a `Clickhouse` instance ready somewhere (no instructions given for this yet)

1. Run the sink

    > **Note** To connect to Substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) to obtain one.

    ```shell
    substreams-sink-clickhouse run \
        "clickhouse://dev-node:insecure-change-me-in-prod@localhost:8123" \
        "mainnet.eth.streamingfast.io:443" \
        https://github.com/streamingfast/substreams-eth-block-meta/releases/download/v0.4.1/substreams-eth-block-meta-v0.4.1.spkg \
        db_out
    ```

### Output Module

To be accepted by `substreams-sink-clickhouse`, your module output's type must be a [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-database-change/blob/develop/proto/substreams/sink/database/v1/database.proto#L7) message. The Rust crate [substreams-data-change](https://github.com/streamingfast/substreams-database-change) contains bindings and helpers to implement it easily. Some project implementing `db_out` module for reference:
- [substreams-eth-block-meta](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/lib.rs#L35) (some helpers found in [db_out.rs](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/db_out.rs#L6))

By convention, we name the `map` module that emits [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-database-change/blob/develop/proto/substreams/sink/database/v1/database.proto#L7) output `db_out`.

### Clickhouse DSN

The connection string is provided using a simple string format respecting the URL specification. The DSN format is:

```
    clickhouse://<user>:<password>@<host>:<port>/<dbname>[?<options>]
```

Where <options> is URL query parameters in <key>=<value> format, multiple options are separated by & signs. Supported options can be seen on libpq official documentation. The options <user>, <password>, <host>, <port> and <dbname> should not be passed in <options> as they are automatically extracted from the DSN URL.

### Improvements

Current implementation of substreams-sink-clickhouse uses the http interface which has better language support than the native one. However, it is more limited than the native interface and worse performances.

Reimplementing this sink using the native interface would improve performance.
