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

### The original baseline (weasel#565, before #577)

Every session ran the `JsonNode.DeepEquals` fallback unconditionally.

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

### After weasel#577

The fallback moved behind `IStorageSession.UseSemanticJsonChangeDetection`, off by default. Both
tables below were re-recorded together on the machine described above, so they compare to each other
rather than to the numbers up top; wall clock on this machine drifts by up to 2x between runs, and
the allocation column reproduced exactly.

| Path | Before | After | Alloc before | Alloc after |
|---|---:|---:|---:|---:|
| Unchanged (ordinal string compare wins) | 3.83 us | 4.09 us | 8.42 KB | 8.42 KB |
| Reordered but equal (default: reports a change) | 31.09 us | **4.08 us** | 82.23 KB | **8.42 KB** |
| Changed (full detection, then `Upsert`) | 15.67 us | **3.81 us** | 30.06 KB | **8.42 KB** |
| Reordered but equal (opt-in fallback) | — | 32.55 us | — | 82.23 KB |

Two things fall out of this that are worth stating separately.

The reordered row does what it was meant to: 7.6x less time and **9.8x less allocation**, because
the two `JsonNode` trees are simply never built. What it buys that with is a redundant write of
semantically identical JSON — a cost the tracker is willing to pay, because the check being skipped
is one that only ever turns "changed" into "unchanged" and so can never lose a write, only add one.
A session that would rather pay the 32.55 us sets the flag and gets the old behaviour back, to the
byte.

The **Changed** row was the unadvertised win. Every genuinely dirty document used to parse both JSON
documents and walk both trees before it was allowed to conclude what the string compare had already
told it, so detecting a real change cost 4x an unchanged one. It now costs slightly *less* than the
unchanged path — same one serialization, and the string compare bails at the first differing byte
instead of running to the end.

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

Measured before and after weasel#573, which routed `SystemTextJsonSerializer`'s reader overloads
through `GetStream` on providers that support it.

| Path | 5 KB | 120 KB | Alloc 5 KB | Alloc 120 KB | Gen2 @ 120 KB |
|---|---:|---:|---:|---:|---:|
| `serializer.FromJson<T>(reader, index)` — **before** (`GetString` + parse) | 25.55 us | 322.78 us | 33.58 KB | 558.85 KB | 76.66 |
| `serializer.FromJson<T>(reader, index)` — **after** (streams, with fallback) | 22.94 us | 349.59 us | **26.74 KB** | **438.54 KB** | **38.09** |
| `GetStream` → `Deserialize(Stream)` directly, no capability check | 23.76 us | 342.06 us | 26.74 KB | 438.54 KB | 38.09 |
| *Rejected:* `GetTextReader` → hand transcode → `Deserialize(span)` | 23.81 us | 316.96 us | 29.99 KB | 441.79 KB | 38.09 |

**The change is an allocation and GC-pressure win, not a throughput win.** It removes **20% of
allocated bytes** at 5 KB and **21% at 120 KB** — the full-size string `GetString` built and STJ
discarded microseconds later — and at 120 KB it **halves the Gen2 collections**, because 120 K
characters is 240 KB of UTF-16 and landed on the large object heap on every single read. Wall-clock
is a wash in both directions and inside this machine's noise; do not quote it as a speedup.

The third row is the same read with the capability check taken out. It allocates identically to the
shipped path, which is the check confirming it costs nothing after the first call — and, on this
provider, that the shipped path really is streaming rather than quietly falling back.

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

## Parameter-type lookup (weasel#804)

`TryGetDbType(Type)`, once per bound parameter on the LINQ `CommandParameter` path and on
`AddNamedParameter` / `AddParameters`. Recorded 2026-11-13 on the machine above, but under
**.NET 10.0.14** (SDK 11.0.100) and the checked-in short job (3 warmup + 5 measured iterations):

```bash
dotnet run -c Release -f net10.0 --project src/Weasel.Benchmarks -- --filter '*ParameterTypeBenchmarks*'
```

The issue proposed freezing the map (`FrozenDictionary`) or making it copy-on-write. It was already
copy-on-write — an `ImHashMap` behind a `Ref`, read lock-free — and the `FrozenDictionary` row below
is the ceiling for freezing it: about 2 ns, which the memo matches once the two real defects are
fixed. Neither defect was the data structure.

| Type | PostgreSQL before | PostgreSQL after | SQL Server before | SQL Server after | Frozen ceiling |
|---|---:|---:|---:|---:|---:|
| `int` | 5.9 ns / 64 B | 1.1 ns / 0 B | 4.5 ns / 32 B | 2.3 ns / 0 B | 2.0 ns / 0 B |
| `long` | 6.8 ns / 64 B | 1.7 ns / 0 B | 4.1 ns / 32 B | 1.6 ns / 0 B | 2.0 ns / 0 B |
| `string` | 6.1 ns / 64 B | 1.6 ns / 0 B | 4.5 ns / 32 B | 2.6 ns / 0 B | 2.0 ns / 0 B |
| `Guid` | 6.4 ns / 64 B | 1.6 ns / 0 B | 5.2 ns / 32 B | 1.8 ns / 0 B | 2.0 ns / 0 B |
| `DateTime` | 6.8 ns / 64 B | 2.3 ns / 0 B | 4.4 ns / 32 B | 2.3 ns / 0 B | 2.0 ns / 0 B |
| `DateTimeOffset` | 6.7 ns / 64 B | 2.2 ns / 0 B | 3.3 ns / 32 B | 0.9 ns / 0 B | 2.3 ns / 0 B |
| `bool` | 6.0 ns / 64 B | 1.1 ns / 0 B | 4.1 ns / 32 B | 2.2 ns / 0 B | 2.1 ns / 0 B |
| `decimal` | 6.3 ns / 64 B | 2.0 ns / 0 B | 4.2 ns / 32 B | 1.7 ns / 0 B | 2.0 ns / 0 B |
| `int?` | 6.5 ns / 64 B | 1.9 ns / 0 B | 5.0 ns / 32 B | 2.3 ns / 0 B | 2.0 ns / 0 B |
| enum | **468.9 ns / 968 B** | 9.7 ns / 0 B | 18.4 ns / 64 B | 10.3 ns / 0 B | 2.0 ns / 0 B |
| enum? | **1,038.1 ns / 1,872 B** | 42.8 ns / 32 B | 79.4 ns / 160 B | 79.9 ns / 64 B | 2.4 ns / 0 B |
| `int[]` | **455.2 ns / 936 B** | 9.5 ns / 0 B | 6.4 ns / 32 B | 3.9 ns / 0 B | 2.1 ns / 0 B |
| `string[]` | **454.9 ns / 936 B** | 10.4 ns / 0 B | 6.5 ns / 32 B | 3.8 ns / 0 B | 2.0 ns / 0 B |
| `List<int>` (PostgreSQL only) | **582.4 ns / 1.13 KB** | 134.3 ns / 184 B | — | — | — |

The two defects:

- **Every lookup allocated a closure, hit or miss.** The memo helpers wrote back through
  `Swap(d => d.AddOrUpdate(type, ...))`, and a lambda that captures a method's *parameter* makes the
  compiler allocate its closure at the top of the method — so a memo hit that never reached the
  `Swap` still paid 32 bytes, and PostgreSQL, with a second such method, paid 64. Moving the writes
  into their own methods takes every memo hit to zero on all six providers
  (`Weasel.Core.Tests/parameter_type_lookups_do_not_allocate.cs` holds them there).
- **PostgreSQL rescanned the Npgsql mapping registry for every enum and array, every time.** The memo
  stores "Npgsql has no mapping" as null and could not tell it from "never asked", so those types
  took every lock on a `ConcurrentDictionary`, copied its values and walked ~80 mappings with
  `LastOrDefault` on each lookup. The miss is now remembered, stamped with the registry's version, so
  a mapping registered later still takes effect — which is the only reason the rescan was there.

What is left: a nullable *unmapped* type still allocates once per lookup in JasperFx's
`GetInnerTypeFromNullable` (a `GetGenericArguments()` array) — 32 B on PostgreSQL's enum?, 64 B on
SQL Server's — and `List<T>` still enumerates `ImplementedInterfaces` on PostgreSQL. Neither is
common enough to be worth a change. SQL Server's `TryGetDbType(typeof(List<int>))` **throws**
rather than returning null, which is why it has no row; that is a behaviour question, not a
performance one.

## String formatting on the command and DDL paths (weasel#805)

Same machine, runtime and job as the section above.

```bash
dotnet run -c Release -f net10.0 --project src/Weasel.Benchmarks -- --filter '*FormattingBenchmarks*'
```

| Path | Mean | Allocated |
|---|---:|---:|
| PostgreSQL `CommandBuilder`: 10 params + `Compile` | 245.9 ns | 3,456 B |
| SQL Server `CommandBuilder`: 10 params + `Compile` | 285.2 ns | 3,224 B |
| `PostgresqlObjectName.ToString()` | 379.7 ns | 240 B |
| `SqlServerObjectName.ToString()` | 4,294.9 ns | 56 B |
| PostgreSQL `Table.WriteCreateStatement` (10 cols, 2 idx, 1 fk) | 9,386 ns | 20,617 B |
| SQL Server `Table.WriteCreateStatement` (10 cols, 2 idx, 1 fk) | 88,187 ns | 17,696 B |
| PostgreSQL `TableDelta.WriteUpdate` (+2 cols, +2 idx, +1 fk) | 4,743 ns | 9,249 B |
| SQL Server `TableDelta.WriteUpdate` (+2 cols, +2 idx, +1 fk) | 62,604 ns | 9,088 B |

**No formatting change was justified.** On the per-command path there is no number-to-string
formatting left to convert: parameter names come from the #558 table (0 B), PostgreSQL's `$N` goes
through `StringBuilder.Append(int)`, which already formats into the builder's buffer, and what the
command rows allocate is the driver's command and parameter objects plus the one SQL string
`Compile` has to produce. On the DDL path the interpolations are of identifier *strings*, and an
interpolated number already formats through `DefaultInterpolatedStringHandler` without an
intermediate string; the DDL runs once per migrated object, not per command.

The SQL Server rows are an order of magnitude slower than PostgreSQL's, and that is not formatting
either: `SqlServerIdentifierRules.IsReservedWord` is a linear `Contains` over ~185 keywords with
`StringComparer.InvariantCultureIgnoreCase`, measured at 4.1-4.7 us per identifier quoted. It is a
DDL-time cost and a separate change, because an ordinal comparison is not a drop-in replacement for a
culture-aware one.
