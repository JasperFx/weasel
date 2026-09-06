# Weasel benchmark baselines

Numbers recorded at the introduction of this project (weasel#565), so later changes have something
to be compared against. **A number here is only meaningful next to the machine it came from** — see
[The machine](#the-machine). Re-record on your own hardware before drawing a conclusion from a
change; compare ratios rather than absolute times.

## Running them

```bash
# The checked-in configuration: 3 warmup + 5 measured iterations per benchmark.
# The whole suite takes a couple of minutes.
dotnet run -c Release -f net9.0 --project src/Weasel.Benchmarks

# One class at a time
dotnet run -c Release -f net9.0 --project src/Weasel.Benchmarks -- --filter '*JsonRead*'

# The full-fidelity run: BenchmarkDotNet's default job (pilot + overhead + ~15 measured
# iterations, with outlier analysis). Roughly 20 minutes for the whole suite. This is what
# the numbers below came from, and what you should use before claiming a regression or a win.
dotnet run -c Release -f net9.0 --project src/Weasel.Benchmarks -- --full
dotnet run -c Release -f net9.0 --project src/Weasel.Benchmarks -- --full --filter '*JsonRead*'
```

Everything runs with `[MemoryDiagnoser]`. `net10.0` also works — pass `-f net10.0`.

No benchmark needs a container. `SchemaMigrationBenchmarks` and `JsonReadBenchmarks` create a
temporary SQLite database file and delete it afterwards.

### Reading these numbers honestly

**Allocation is the trustworthy column here.** It is deterministic, and it reproduced exactly
across every run. The wall-clock times moved by as much as 2x between runs of the same benchmark on
this machine, because it was not quiet (a few dozen containers were running). Where a conclusion
below rests on timing, it says so, and it is stated as a range rather than a figure.

## The machine

| | |
|---|---|
| CPU | Apple M5 Max, 18 physical / 18 logical cores |
| OS | macOS Tahoe 26.4.1 (Darwin 25.4.0) |
| SDK | .NET SDK 10.0.101 |
| Runtime | .NET 9.0.8, Arm64 RyuJIT armv8.0-a |
| GC | Concurrent Workstation |
| BenchmarkDotNet | 0.15.8 |
| Recorded | 2026-09-05 |
| Load | **Not a quiet machine** — see above |

---

## ChangeTracker.DetectChanges (~5 KB document)

Dirty tracking calls this once per tracked document on every `SaveChanges`, so a session that loaded
a hundred documents and changed one pays the *unchanged* cost ninety-nine times.

| Path | Mean | Ratio | Allocated | Alloc ratio |
|---|---:|---:|---:|---:|
| Unchanged (ordinal string compare wins) | 4.47 us | 1.00 | 8.42 KB | 1.00 |
| Reordered but equal (`JsonNode.DeepEquals` fallback) | 34.55 us | 7.73 | 82.23 KB | 9.76 |
| Changed (full detection, then `Upsert`) | 17.09 us | 3.82 | 30.06 KB | 3.57 |

The reference document serializes to ~5 KB, and the unchanged path allocates 8.42 KB — one
re-serialization of the document, which is the floor for a design that detects changes by
re-serializing. Nothing here is free, but the common case is the cheap one.

The row worth staring at is the middle one. When two JSON strings differ but mean the same thing —
a dictionary that re-ordered itself between load and save is the usual cause — the ordinal compare
misses, and the fallback builds **two whole `JsonNode` trees and throws them both away** to conclude
that nothing changed: 7.7x the time and **9.8x the allocation of the unchanged path**, to produce
the same answer. It is also more expensive than genuinely detecting a change (row 3), because a real
change is usually visible early in the tree while equality has to walk all of it.

## BatchBuilder.AppendParameter x 500

| Builder | Mean | Ratio | Allocated | Alloc ratio |
|---|---:|---:|---:|---:|
| SQL Server (`ParameterNames` table) | 10.43 us | 1.00 | 95.49 KB | 1.00 |
| PostgreSQL (`$N`, never named) | 9.22 us | 0.88 | 105.30 KB | 1.10 |

Both builders sit around 20 ns per parameter, and what is left is the driver's own parameter object
plus the SQL text — not the naming. The precomputed name table from #558 did its job: SQL Server
names all 500 parameters and still allocates *less* than PostgreSQL, which names none.

### The name table in isolation

| Lookup | Mean | Allocated |
|---|---:|---:|
| `ParameterNames.ForPosition` x 500, inside the 512-entry table | 285 ns | **0 B** |
| `ParameterNames.ForPosition` x 500, past the table (concatenates) | 4,421 ns | 32,000 B |

This is the #558 change measured directly, and the second row is what *every* position cost before
it: **15x the time and 32 KB of garbage per 500 parameters**. It also prices the table's edge —
a command binding more than 512 parameters pays the old cost from 512 onward. Nothing in Weasel
approaches that (SQL Server's hard limit is 2100 and a table's introspection query binds two), but
if a caller ever does, the cliff is here and it is measured.

## SchemaMigration.DetermineAsync over 200 tables

Against a real SQLite database file, so the render is measured next to the round trip it feeds.

| Scenario | Mean | Ratio | Allocated | Alloc ratio |
|---|---:|---:|---:|---:|
| 200 tables, schema already matches | 17.84 ms | 1.00 | 3.70 MB | 1.00 |
| 200 tables, empty database | 9.67 ms | 0.54 | 2.06 MB | 0.56 |

About 89 us and 18 KB per table for the steady-state case — the one a healthy application start
hits, where every table exists as configured and the entire cost is introspection and comparison.
The empty database is roughly half of that, because every table comes back missing and there is
nothing to compare it against.

These are *post*-#560 numbers, and #560's whole subject is how many times each object renders. There
is no pre-change measurement to set beside them, because the benchmark did not exist when the change
landed — which is the reason this project does. Treat the 17.84 ms as the line to defend.

## JSON reads

### The deserialize step alone (payload already in memory)

This is the measurement that decides whether reaching a column's UTF-8 bytes is worth any plumbing:
if `ReadOnlySpan<byte>` and `string` cost the same, no amount of stream routing can help.

| Source | 5 KB | 120 KB | Allocated (5 KB / 120 KB) |
|---|---:|---:|---:|
| `Deserialize<T>(string)` — what `GetString` feeds | 19.92 us | 284.25 us | 19.12 KB / 317.43 KB |
| `Deserialize<T>(ReadOnlySpan<byte>)` — UTF-8, no transcode | 13.93 us | 277.66 us | 19.12 KB / 317.43 KB |
| `Deserialize<T>(Stream)` — what `GetStream` feeds | 17.89 us | 289.73 us | 19.18 KB / 317.49 KB |
| `UTF8.GetBytes(string)` + `Deserialize(span)` — the transcode, priced | 16.35 us | 289.52 us | 26.17 KB / 437.97 KB |

Two things to take from this:

- **Allocation is identical** across the first three rows (the payload is pre-built, so what is
  counted is the object graph only). Whatever the read path saves, it saves on the *string*, not on
  the parse.
- **Parsing UTF-8 is somewhat cheaper than parsing UTF-16 at 5 KB and roughly a wash at 120 KB.**
  The last row is the one that settles the design question: transcoding by hand and then parsing the
  span costs about what parsing the string costs, and allocates 38% more. Moving the transcode from
  inside STJ to outside it buys nothing.

### The whole read, query included (SQLite, TEXT column)

| Path | 5 KB | Ratio | 120 KB | Ratio | Alloc 5 KB | Alloc 120 KB |
|---|---:|---:|---:|---:|---:|---:|
| **Current** — `serializer.FromJson<T>(reader, index)` (`GetString` + parse) | 25.55 us | 1.00 | 322.78 us | 1.00 | 33.58 KB | 558.85 KB |
| `GetStream` → `Deserialize(Stream)` | 20.24 us | 0.82 | 351.47 us | 1.09 | **26.74 KB** | **438.54 KB** |
| *Rejected:* `GetTextReader` → hand transcode → `Deserialize(span)` | 20.37 us | 0.82 | 324.89 us | 1.01 | 29.99 KB | 441.79 KB |

**The stream path is an allocation and GC-pressure win, not a throughput win.** It removes ~20% of
allocated bytes at both sizes — the full-size string that `GetString` builds and STJ discards
microseconds later. At 120 KB it also halves the Gen2 collections (38.1 vs 76.7 per 1000 ops),
because a 120 K-character string is 240 KB of UTF-16 and lands on the large object heap on every
single read. Wall-clock is a wash: better at 5 KB, slightly worse at 120 KB, and both differences
are inside this machine's noise.

### Provider support for `GetStream`

Measured against live databases rather than assumed, because the answer is not what the method's
presence on `DbDataReader` suggests:

| Provider | Column type | `GetStream` | `GetTextReader` |
|---|---|---|---|
| SQLite (Microsoft.Data.Sqlite) | `TEXT` | works | works |
| PostgreSQL (Npgsql 9) | `jsonb`, `json`, `text` | works | works |
| SQL Server (Microsoft.Data.SqlClient 6.1) | `nvarchar(max)` | **`InvalidCastException`** | works |
| SQL Server (Microsoft.Data.SqlClient 6.1) | `json` (SQL Server 2025 native) | **`InvalidCastException`** | works |

> Invalid attempt to GetStream on column 'x'. The GetStream function can only be used on columns of
> type Binary, Image, Udt or VarBinary.

Both SQL Server results hold under `CommandBehavior.Default` and `CommandBehavior.SequentialAccess`.
Marten can use `reader.GetStream(index)` because Npgsql is the only provider it targets; the same
call is not portable to the shared serializer in Weasel.Core, and any change to that read path has
to keep a string fallback for SQL Server. SQL Server also sends both column types as UTF-16 on the
wire, so there is no UTF-8 there to reach for — the string path is already the right one.

Two smaller findings from the same probe, recorded so nobody has to rediscover them:

- `GetString` still works on a column after `GetStream` has thrown `InvalidCastException` on it, in
  both command behaviors. A try-stream-then-fall-back-to-string read path is therefore safe.
- `GetFieldValueAsync<TextReader>` throws `NullReferenceException` on SQL Server 2025's native
  `json` column under `CommandBehavior.SequentialAccess`. It works on `nvarchar(max)`, and it works
  on `json` under `CommandBehavior.Default`. Prefer the synchronous `GetTextReader`.
