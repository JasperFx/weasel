# Upgrading to 9.34

9.34 fixes two ways a schema could be wrong without saying so. One is a binary break that 9.33.0
shipped by accident and that only surfaces when a host boots; the other is an EF Core migration that
applies cleanly and creates the wrong index.

::: danger Upgrade from 9.33.0 — it is binary-breaking for Weasel.SqlServer consumers
9.33.0 removed four public `SharedLockExtensions` signatures. Anything compiled against 9.32.0 or
earlier — **current `WolverineFx.SqlServer` among them** — throws `MissingMethodException` at
runtime when 9.33.0 is resolved underneath it. 9.34.0 restores the signatures.

If you are on 9.33.0 with `WolverineFx.SqlServer` or Polecat, move to 9.34.0.
See [below](#the-9-33-0-binary-break-is-repaired).
:::

::: tip Coming from 9.32?
You skipped the break entirely; 9.34 is additive over 9.32 apart from the EF Core index change.
:::

## The 9.33.0 binary break is repaired

[#616](https://github.com/JasperFx/weasel/issues/616).

[#607](https://github.com/JasperFx/weasel/pull/607) added a defaulted `lockTimeoutMs` parameter to
four public `SharedLockExtensions` helpers. Adding an optional parameter is **source** compatible and
**binary** breaking: C# bakes the full parameter list into the call site, so an already-compiled
caller binds to a signature that is no longer in the assembly.

| Method | Shipped through 9.32.0 | 9.33.0 |
|---|---|---|
| `GetGlobalTxLock` | `(SqlTransaction, String, CancellationToken)` | added `Nullable<Int32>` |
| `TryGetGlobalTxLock` | `(SqlTransaction, String, CancellationToken)` | added `Nullable<Int32>` |
| `GetGlobalLock` | `(SqlConnection, String, CancellationToken, SqlTransaction)` | added `Nullable<Int32>` |
| `TryGetGlobalLock` | `(SqlConnection, String, CancellationToken)` | added `Nullable<Int32>` |

`WolverineFx.SqlServer` 6.40.0 calls the three-argument form and declares a `9.32.0` floor. Polecat
5.31.0 floors `9.33.0`. An application on both resolves 9.33.0 under a caller compiled against the
removed signature and fails the moment a host boots and takes the lock:

```
System.MissingMethodException: Method not found: 'System.Threading.Tasks.Task`1<Boolean>
Weasel.SqlServer.SharedLockExtensions.TryGetGlobalLock(
    Microsoft.Data.SqlClient.SqlConnection, System.String, System.Threading.CancellationToken)'
```

**Nothing catches this before runtime**, which is why it escaped. Restore is clean, because 9.33.0
satisfies the declared `9.32.0` floor — no `NU1109`, no warning. Compile is clean, because the caller
is already compiled — no `CS` error. And `Weasel.Postgresql.AdvisoryLockExtensions` was untouched, so
a green Marten/Postgres suite says nothing at all about it.

9.34.0 restores the four signatures as forwarders. Nothing else changes: they pass the same `null`
the current overloads already default to, and `lockTimeoutMs` keeps working exactly as 9.33.0
introduced it. No source change is needed in either direction.

## EF Core: an index Weasel cannot express as a typed operation now carries its own DDL

[#615](https://github.com/JasperFx/weasel/issues/615), reported with a complete repro by
[@Jaxter](https://github.com/Jaxter).

`db-ef-migration` translated every index into a typed `CreateIndexOperation`, whose `Columns` is a
list of identifiers that the EF provider quotes one at a time. Two things went wrong with that, and
only one of them was noisy.

### The loud one: a computed index failed to apply

Marten's `ComputedIndex` carries its expression as an entry in `Columns`. EF quoted it as an
identifier, so the generated migration died on apply:

```
42703: column "(data ->> 'Kind')" does not exist
```

### The quiet one: an operator class was silently dropped

`CreateIndexOperation` has nowhere to put an operator class. A Marten `GinIndexJsonData()` index —

```sql
CREATE INDEX mt_doc_gadget_idx_data ON repro.mt_doc_gadget USING gin (data jsonb_path_ops);
```

— was generated as `USING gin (data)`, which applies **without error** using the default `jsonb_ops`.
A different index from the one declared, on a migration that reported success. The same subset was
all `SnapshotIndex` captured, so the differ compared it too and reported "unchanged" whenever one of
those options later changed.

### What 9.34 does

An index the typed operation cannot reproduce is emitted as a `migrationBuilder.Sql(...)` block
carrying its own `CREATE INDEX`. That covers an expression among the key columns, and any option
`CreateIndexOperation` has no place for — a PostgreSQL operator class or mask, sort or nulls order,
collation, tablespace, storage parameters; a SQL Server fill factor, clustering or per-column
direction.

**Only the index takes that route. The table around it stays typed**, so snapshot diffing keeps
working for the rest of it. That is the point of the fix: the previous workaround was
`ForceRawSql` over the whole table, which made every later change to that table something the
differ refused, so each one had to be hand-written as SQL.

A raw-SQL index is also **diffed rather than refused** — unlike other raw-SQL objects, it has DDL to
compare — so changing an operator class or a sort order now produces a migration.

A concurrent build is deliberately *not* treated as such an option: it says nothing about the shape
of the index, and `CREATE INDEX CONCURRENTLY` cannot run inside a migration's transaction anyway.

::: warning Your first migration after upgrading may contain index changes you did not make
An existing snapshot recorded these indexes with the incomplete description. Against a 9.34 model
they no longer compare equal, so the next `db-ef-migration add` emits a drop-and-recreate for each
affected index. That is the correction — it replaces indexes that really are wrong in the database
— but review it rather than assuming it is spurious.

One caveat on the way through: the `Down()` of that particular migration reverts to the *old*
snapshot's description of the index. For an operator-class index that is correct (it restores the
pre-9.34 index). For a computed index recorded before 9.34, the `Down()` reproduces the expression
as a column name and would fail with `42703` — but a migration containing such an index could never
have been applied in the first place, so there is no deployed state to revert to. Later migrations
are unaffected, since both sides then carry the DDL.
:::

## `ITableIndex` gains two members

To let `Weasel.EntityFrameworkCore` — which references only `Weasel.Core` — ask an index whether the
provider-neutral surface actually describes it, `ITableIndex` gains:

```csharp
bool HasProviderSpecificOptions { get; }
string ToDDL(ITable parent);
```

All five providers implement both. **This is a source break only if you implement `ITableIndex`
yourself**, which is unusual — the interface exists to be consumed, and every shipped implementation
is a Weasel provider's own `IndexDefinition`. If you do, implement `HasProviderSpecificOptions` as
`false` and `ToDDL` as your own `CREATE INDEX` rendering.

`ToDDL(ITable)` must return a single statement that is safe to embed in a migration script: never a
multi-statement or concurrent form, and never carrying Weasel's own marker comments. PostgreSQL's
implementation returns the non-concurrent rendering for exactly that reason.

## Also in this release

A test-infrastructure fix, with no effect on the shipped packages:
`InvertedComparisonHarness` was relying on whatever assemblies the test host happened to have
loaded, so the EF Core inverted comparison suite passed in a full run and failed with
`CS0012: 'Expression<>' ... not referenced` when a single test was run on its own.
