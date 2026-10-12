# SQL Server Receiver

<!-- markdownlint-configure-file { "MD013": { "tables": false } } -->

## Status

This is an **experimental query-polling receiver**. The opt-in `sql-server`
feature registers `urn:otel:receiver:sql_server` and provides validated configuration,
T-SQL preparation, semantic fingerprints, capability-based credential
acquisition, metadata discovery, normalized page fetching, cancellation,
and a `DriverAdapter` implementation for the shared controller.

It uses a temporary immutable Tiberius revision containing the small upstream
[provider-neutral TLS fix][tiberius-pr]. The manifest links the upstream PR and
pins commit `8bbd59fe9e01a74b6f790756077c8b0e09605c68` with
`default-features = false` and `tds73,rustls-no-provider`. Return to a crates.io
release once it includes this fix. Do not enable Tiberius's legacy `rustls` or
`rustls-webpki-roots` features: they intentionally retain AWS-LC for existing users.

`TcpConnector` establishes verified TLS using the host-installed crypto provider,
platform certificate roots, and an optional additional CA file. There is no
plaintext mode or certificate-verification bypass. The factory requires one
pipeline core and the `basic_auth_provider` capability and acquires shared
checkpoint ownership before starting.

This work follows [database polling RFC #3918][rfc]. It reuses shared scraper
configuration and query contracts without changing Oracle behavior.

## Driver Qualification Gate

The upstream TLS fix removes the forced-backend dependency for this integration;
it does not establish complete production readiness. The driver still
buffers partially length-prefixed values up to `i32::MAX` bytes rather
than a receiver-selected budget. Its raw money decoder also uses floating
point. A post-conversion row-size check cannot fix these driver-level limits.

Before marking the receiver ready, resolve and demonstrate:

- Value, row, metadata, and protocol-buffer bounds before allocation and while
  reading, including MAX values and malformed responses.
- Bounded cancellation and confirmed safe cleanup, with no overlapping
  replacement operation after uncertain cancellation.
- Exact metadata/type handling, strict text decoding, and redacted diagnostics.
- Broader deployment qualification beyond the documented SQL Server 2022
  Linux-container fixture.

The receiver does not require customer-installed ODBC/JDBC drivers, SQL tools,
a JVM, or native database clients. Containers are test infrastructure, not a
production deployment requirement. Distributors should ship prebuilt binaries
or images with the feature enabled; customers should not need a Rust toolchain.

## Getting Started

For a guided local demonstration, see [Demo: SQL Server rows to OpenTelemetry
logs](DEMO.md). It covers fixture setup, two-row catch-up pages, inserting new
data, checkpoint inspection, restart/resume, and cleanup in two PowerShell
terminals.

Use a dedicated SQL login with CONNECT access to the explicitly configured
database and SELECT access only to the intended table/view. Metadata discovery
requires permission to execute that same query; no blanket administrator grant
is required. Provision a server certificate trusted by the host or supply its
CA through `connection.tls.ca_file`. Certificate identity must match
`connection.host`.

Enable `sql-server` in the engine build; the root feature also enables the
flat-file credential extension. The normal host initialization installs the
selected `crypto-*` provider before any receiver starts. An embedding host must
do the same. Missing provider selection is an explicit error, not a fallback.

The complete runnable pipeline is
[`configs/sql-server-console.yaml`][example]. It binds `basic_auth_provider` to
`flat_file_user_pass_auth`, takes a mounted password file, and uses
`SQL_SERVER_STATE_DIR` for persistent checkpoint files (default `./state/sql_server`).
The optional CA file augments platform roots and is
limited to 4 MiB. Omit the TLS block when platform trust is sufficient.

The console destination demonstrates collection, not durable delivery.
Replace it with a feedback-preserving path to a durable destination when relying
on acknowledged checkpoints. One active poller per logical source range and
stop-before-start replacement are required; checkpoint locks do not detect
equivalent queries under other pipeline identities or state directories.

Incremental at-least-once behavior also requires unique, immutable,
commit-visible cursor ordering, unchanged selected values, and retention
covering the replay window. IDENTITY/SEQUENCE allocation, rowversion, or
application timestamps alone do not prove commit order. Snapshots are periodic
observations, not CDC or guaranteed replay of transient rows.

Use the receiver's checkpoint directory directly on Windows. The engine's
optional managed `engine.state_dir` provisioning is Linux-only; setting that
engine option on Windows is rejected before receivers start.

## Adapter Behavior

`SqlServerAdapter<C>` implements the existing shared `DriverAdapter`. Its futures,
session ownership, and generation-specific cancellation remain local. Tiberius
requires a `Send` transport, not a multithreaded receiver runtime or session pool.

The adapter acquires the current `BasicAuthProvider` snapshot on connect/reconnect,
rejects near-expiry credentials, constructs SQL authentication directly, and
requests `EncryptionLevel::Required`. The connector must honor verified TLS and
the configured CA file; it must not silently relax this contract.
Credential acquisition and connection establishment share one timeout.

Tiberius loads platform roots synchronously inside its TLS handshake.
Connection establishment therefore uses at most one owned worker thread per
receiver, with no queued jobs and no Tokio blocking-pool job. It uses the
pipeline's existing Tokio reactor, not a second runtime. After the handshake,
the client is transferred to the local pipeline for all query I/O. Dropping
the connection future signals cancellation; shutdown confirms worker completion.
An unconfirmed exit fails cleanup rather than permitting overlapping ownership.

Metadata discovery uses parameterized `sys.dm_exec_describe_first_result_set`,
ordered by column ordinal, with typed parameter declarations. It validates
names, NOT NULL cursors, precision/range, supported types, and bounded metadata.
Each execute re-describes the query and compares its full plan before reading.
The execution deadline covers metadata discovery and fetching together.

Rows are read incrementally through the real Tiberius stream. A complete
snapshot fits or fails. An incremental byte-limited prefix can be returned only
after the unread request is cancelled; the next poll opens and validates a fresh
session. Native allocations have already occurred before normalized-page
accounting: the configured byte limit is **not** a driver or process-memory bound.

Supported body conversions include exact integers, decimal precision through
38 (including decimal(38,38)), finite floats, null/bit, strings, bytes, GUID text,
date/time, datetime2, and datetimeoffset. Raw money, legacy datetime types, XML,
UDTs, variants, and unsupported metadata fail explicitly.
Cursor binds check native signed-integer range and datetime2 precision.

Cancellation signals one operation, attempts TDS ATTENTION under a one-second
cleanup cap, and discards the session. Failed cancellation confirmation prevents
another operation and makes shutdown fail, allowing shared ownership quarantine.
This is not proof of server-side termination or exhaustive cancellation safety.
Transient transport/server codes use the shared retry policy; authentication,
TLS, metadata, conversion, and deadline failures are not retried automatically.

Stock-driver tracing is suppressed while driver futures are polled because it
can contain rows and raw server errors. The shared controller still receives
redacted errors for its normal diagnostics. The connection worker applies the
same suppression. Embedding connectors must preserve these diagnostic and
cleanup contracts.

## Configuration Contract

The following configuration goes inside the SQL Server receiver's `config`
block. Unknown fields are rejected, and
errors and `Debug` formatting redact SQL, endpoints, and cursor values.

```yaml
source_id: sql-events
connection:
  host: sql.example.test
  database: telemetry
query:
  statement: >
    SELECT EVENT_ID, EVENT_TS, PAYLOAD FROM dbo.EVENTS
    WHERE EVENT_TS > @last_timestamp
       OR (EVENT_TS = @last_timestamp AND EVENT_ID > @last_id)
    ORDER BY EVENT_TS ASC, EVENT_ID ASC
  max_rows_per_poll: 1000
  max_batch_bytes: 4MiB
watermark:
  mode: composite
  timestamp:
    column: EVENT_TS
    bind: last_timestamp
    initial: "2026-01-01T00:00:00Z"
    timezone: UTC
  tie_breaker:
    column: EVENT_ID
    bind: last_id
    initial: 0
checkpoint:
  directory: state/sql-server
  on_nack: rewind
  nack_backoff: 1s
  max_consecutive_failures: 5
```

| Field | Validation/default |
| --- | --- |
| `source_id` | 1..256 ASCII letters, digits, `_`, `-`, or `.`. Keep IDs non-sensitive. |
| `connection.host` | DNS name or unbracketed IP address, not a URL, named instance, or connection string. DNS names are lowercased for identity. |
| `connection.database` | Required nonempty name, no control characters, at most 128 UTF-16 code units. |
| `connection.port` | Default 1433; 1..65535. |
| `connection.connect_timeout` | Default 10s; 1s..5m, whole seconds. Total credential/DNS/TCP/TLS/login attempt budget. |
| `connection.tls.ca_file` | Optional nonempty path; supplied null is rejected. The file is not read during preparation. No insecure-verification option exists. |
| `query.interval` | Default 1m; 1m..24h, whole seconds. |
| `query.timeout` | Default 30s; 1s..5m, whole seconds. Total metadata/fetch operation budget, not an ACK deadline. |
| `query.fetch_size_rows` | Default 300; 1..`max_rows_per_poll`. Cooperative decode-chunk bound, not a TDS packet size. |
| `query.max_rows_per_poll` | Required; 1..10000. |
| `query.max_batch_bytes` | Required; 1 byte..256 MiB, numeric or size string. Shared normalized and serialized budgets remain separate, not an RSS ceiling. |
| `query.catch_up` | Existing shared defaults and bounds, including 32 pages and 10s elapsed admission time. |
| `checkpoint` | Existing shared file store and policy, including permanent-NACK pause by default. The factory acquires exclusive checkpoint-identity ownership. |

Credentials are intentionally absent from receiver configuration. The factory
requires `BasicAuthProvider`; the adapter passes its username/password directly
to Tiberius. Refreshing a password does not force an open session to reauthenticate.
New connections use the current credential snapshot, without resetting progress.

### Query Modes

Composite mode uses the exact lexicographic timestamp/integer predicate above.
Scalar mode uses one strict comparison and matching ascending ordering:

```yaml
watermark:
  mode: scalar
  column: EVENT_ID
  bind: last_key
  initial:
    type: int64
    value: 0
```

```sql
SELECT EVENT_ID, PAYLOAD FROM dbo.EVENTS
WHERE EVENT_ID > @last_key
ORDER BY EVENT_ID ASC
```

Scalar initial types are `int64` or `timestamp`; uint64 and string cursors are
rejected. Snapshot uses only `watermark: { mode: snapshot }`, with no cursor
parameters, for example:

```sql
SELECT STATUS, COUNT(*) AS TOTAL FROM dbo.EVENTS
GROUP BY STATUS
```

Timestamp initial values are normalized to UTC with seven fractional digits.
Timezone-less `YYYY-MM-DD HH:MM:SS[.fraction]` has UTC semantics. Nonzero
precision finer than 100ns, leap seconds, and dates outside years 0001..9999
are rejected. Fractional text exceeding nine digits is rejected before parsing
can truncate it. Representable trailing zeros are accepted.

Live validation checks `datetime2(p)` precision (including p < 7),
signed native integer range, NOT NULL metadata, and supported result types.
Neither this compiler nor monotonically increasing IDs prove uniqueness,
commit-visible ordering, immutability, or retention. SQL Server `timestamp`
means rowversion, not datetime2, and is not an allowed temporal cursor.

### Restricted SQL

The compiler uses the existing sqlparser version with `MsSqlDialect`, with a
16 KiB SQL ceiling, 2048-token ceiling, bounded parser/expression recursion,
and at most 256 projected columns.

It accepts one schema-qualified table or view, regular or bracketed identifiers,
explicit named projections, and an optional final semicolon. Cursor identifiers
must have exactly the configured spelling in projection, predicate, and ordering;
this avoids equating distinct columns in case-sensitive SQL Server databases.
ASC may be omitted. Cursor projections cannot be aliased or derived.

Allowed body expressions are literals, supported arithmetic/comparison/logical
operators, null tests, and the functions `ABS`, `COALESCE`, `ISNULL`, `LOWER`,
`UPPER`, `LEN`, and `DATALENGTH`. Snapshot additionally supports `COUNT`,
`COUNT_BIG`, `MIN`, `MAX`, `SUM`, and `AVG`, with WHERE/GROUP BY/HAVING and
optional ordering. Expressions require explicit output aliases.
`CAST(... AS DECIMAL(p,s))` supports p=1..38 and s=0..p, including a lossless
money-to-`decimal(19,4)` projection.

Joins/table aliases, subqueries, CTEs, set operations, ALL/DISTINCT, wildcards,
user TOP/OFFSET/FETCH, hints, SELECT INTO, variable assignments, dynamic SQL,
stored procedures, qualified/custom functions, and unsupported syntax are
rejected. This subset deliberately fails closed; it is not a general-purpose
SQL Server query editor or a substitute for least-privileged permissions.

### Driver Plan and Compatibility

`query()` retains canonical operator SQL with logical bind names.
`statement().driver_sql()` contains generated TOP and native `@P1`, `@P2`, ...
slots. `statement().parameters()` identifies the role of each native argument.
The adapter uses both the native statement and its binding plan,
binding values using the driver's API, never SQL interpolation.

Composite ordering is timestamp, tie-breaker, page limit. Scalar ordering is
the typed key then the limit; snapshot binds only the limit. The repeated
timestamp comparison reuses one parameter. Parameter-looking text inside
literals or identifiers is never rewritten.

`page_limit()` returns the configured row ceiling for keysets and ceiling plus
one for snapshots. The extra snapshot row detects overflow; the adapter
rejects the entire oversized result, rather than emitting the prefix.
TOP limits returned rows, not necessarily database scan/sort work.

The versioned fingerprint covers database system, source ID, explicit
host/port/database, canonical operator SQL, and typed watermark semantics.
It excludes credentials, CA paths, checkpoint location, timeouts, interval,
and row/byte/catch-up tuning. No current Oracle fingerprint is changed.

## Development Checks

From `rust/otap-dataflow`:

```powershell
cargo check -p otel-arrow-dfe-contrib-nodes --features sql-server
cargo test -p otel-arrow-dfe-contrib-nodes --features sql-server sql_server_receiver
```

These include configuration/compiler tests, in-memory TDS protocol tests, and
real shared-controller ACK/checkpoint coverage. Live tests are explicitly
ignored by default and fail on missing fixture inputs when requested.
The required Linux all-feature test build includes them; the required Windows
test archive explicitly enables `sql-server` while retaining its SymCrypt
feature selection.
For a disposable local live fixture, use Python, OpenSSL, and Docker on a machine
that can run Linux containers. These are developer-only prerequisites:

The fixture checks executable availability and Docker's Linux-container mode
before creating files. On Windows it also discovers OpenSSL bundled with Git
for Windows when `openssl` is not on the terminal's PATH, so another OpenSSL
installation is unnecessary. If an earlier attempt left a partial directory,
choose a new directory for the retry. Do not load `fixture.json` or continue
with receiver startup unless the fixture command succeeds.

```powershell
# From the repository root; choose a new scratch directory outside the checkout.
python rust\otap-dataflow\scripts\sql-server-fixture.py start --directory C:\temp\otel-sql-fixture
$fixture = Get-Content C:\temp\otel-sql-fixture\fixture.json -Raw | ConvertFrom-Json
foreach ($entry in $fixture.env.PSObject.Properties) {
    Set-Item -Path "Env:$($entry.Name)" -Value $entry.Value
}
cd rust\otap-dataflow
cargo test --locked -p otel-arrow-dfe-contrib-nodes `
    --features sql-server sql_server_receiver::live_tests -- --ignored --nocapture
cd ..\..
python rust\otap-dataflow\scripts\sql-server-fixture.py stop --directory C:\temp\otel-sql-fixture
```

The fixture uses SQL Server 2022 build 16.0.4295.3, pinned by image digest in the
script. It binds an ephemeral host port to loopback, generates a short-lived
CA/certificate, creates a restricted reader, and removes only its owned
container and generated secrets on stop. Its setup client also verifies TLS.
For Linux receivers, load the same variables from `fixture.json`; a client
container sharing the server container's network namespace uses `localhost:1433`
and mounts the CA/password files read-only.

The same four live cases have been exercised with native Windows x64 receivers
(Ring and SymCrypt) and a Linux x64 receiver (Ring), against that pinned
Linux-container fixture: all polling modes/type fidelity, invalid CA/hostname
rejection, connection cancellation cleanup, and factory NACK/replay/ACK/restart.
This is **not** qualification of Windows-hosted SQL Server, other server versions,
Azure SQL/Managed Instance, ARM64, or all in-flight query cancellation scenarios.
The Linux integration workflow repeats these cases; the Windows CI test archive
currently includes deterministic tests, not a hosted live Linux-container job.

Driver-internal allocation limits, broader fault/performance qualification,
platform coverage, and packaging remain readiness work. A runnable experimental
receiver must not be described as production-qualified.

[rfc]: https://github.com/open-telemetry/otel-arrow/issues/3918
[tiberius-pr]: https://github.com/tiberius-rs/tiberius/pull/456
[example]: ../../../../../configs/sql-server-console.yaml
