# Upgrading to 9.48

Six fixes, and **one of them changes behaviour you may be relying on**: an enum bound as a parameter
now goes out as an integer by default on every provider. If you store enums as strings on MySQL or
Oracle, read [the enum section](#enums-are-bound-by-their-enumstorage-not-by-a-per-provider-guess)
before upgrading — it is the only breaking change here, and it is a two-line fix at the call site.

Everything else is additive or a straight fix: three SQL Server bugs found from Polecat, the two
remaining loose ends from 9.47.1's parameter-type work, and a documentation-only settlement of a
question that looked like a bug and is not.

## Enums are bound by their `EnumStorage`, not by a per-provider guess

[#761](https://github.com/JasperFx/weasel/issues/761) /
[#774](https://github.com/JasperFx/weasel/pull/774).

`GetDatabaseType` has always generated an **integer** column for an enum under
`EnumStorage.AsInteger` and a **character** column under `EnumStorage.AsString`. The parameter path
had no `EnumStorage` in scope at all, so each provider picked one constant and hoped.

Measured against the column each provider's own `GetDatabaseType` generates — and this is the part
that reframes the issue, because **three of the six did not merely convert the column, they threw**:

| Provider | old parameter type | vs its AsInteger column | vs its AsString column |
| --- | --- | --- | --- |
| SQL Server | `Variant` | **throws**: `Implicit conversion from data type sql_variant to int is not allowed` | **throws** |
| MySQL | `VarChar` | **throws**: `Incorrect integer value: 'Strawberry'` | works |
| Oracle | `Varchar2` | **throws**: `ORA-01722` | works |
| SQLite | `Text` | works — type affinity stores the integer either way | stores the integer, not the name |
| PostgreSQL | `Integer` | works | wrong |
| Firebird | `Integer` | works | wrong |

So on SQL Server this was never
[#758](https://github.com/JasperFx/weasel/issues/758)'s performance bug one type over — binding an
enum was simply impossible. And `AsInteger` is the `JasperFx` default, so three providers were
throwing against the *default* column shape.

### The seam

`ToParameterType`, `TryGetDbType` and `TryGetDbTypeForValue` each gain an `EnumStorage` overload:

```csharp
var dbType = provider.ToParameterType(typeof(Flavor), EnumStorage.AsString);

provider.TryGetDbType(typeof(Flavor), EnumStorage.AsString);
provider.TryGetDbTypeForValue(theFlavor, EnumStorage.AsString);
```

They are **defaulted on `IDatabaseProvider<,,>`**, so a provider implemented outside Weasel keeps
compiling and keeps working.

`AsString` substitutes the provider's `string` mapping. `AsInteger` substitutes the enum's
**underlying** type rather than `int` flatly, so a `long`-backed enum carrying a value past
`int.MaxValue` is not narrowed and a `byte`-backed one is not widened past what its column holds;
`int` is the fallback where a provider has no mapping for that width.

The overloads **without** an `EnumStorage` assume `AsInteger`, and every provider now routes its own
enum branch through that same rule instead of naming a constant. Naming constants independently is
how the six came to disagree in the first place, so a test pins that the two paths agree — per
provider, and per underlying width.

### What changes for you

- **If you store enums as strings on MySQL or Oracle, you must pass the storage.** Those are the two
  providers whose old catch-all happened to send the enum's *name*, which worked against the
  `varchar` column `AsString` generates. Pass `EnumStorage.AsString` at the call site and you get
  exactly the binding you had.
- **That same code was already broken on the other four providers.** SQL Server threw outright, and
  PostgreSQL, Firebird and SQLite bound the integer into a character column. So the set of people
  this breaks is the set for whom it worked on exactly MySQL or Oracle.
- **If you store enums as integers — the default — this is a fix, not a break.** SQL Server, MySQL
  and Oracle could not bind an enum at all before.
- **PostgreSQL and Firebird keep the same answer for an int-backed enum.** `Integer`, as before. Only
  a `long`- or `byte`-backed enum resolves differently, and more correctly.

## Arrays defer to the driver instead of binding as text

Also [#761](https://github.com/JasperFx/weasel/issues/761).

On SQL Server, SQLite, MySQL and Oracle an array — anything but `byte[]` — resolved to the same
character catch-all as an enum, so an `int[]` was quietly bound as text. It is now left **unmapped**,
which on this seam means *defer*: the parameter goes out untyped and the driver rules on it.

The refusal that `#760` revealed sitting unreachable behind the catch-all
(`"Sql Server does not support arrays"`) is deliberately **not** turned on. Deferring is the
established meaning of "unmapped" here ([#404](https://github.com/JasperFx/weasel/issues/404)), it
does not override a driver that can handle an array on its own, and when there is a failure it now
names the real problem — SqlClient says `No mapping exists from object type System.Int32[]` instead
of Weasel binding it to something that merely looks like it worked.

PostgreSQL still maps an array properly as an `Array | element` composite, and Firebird still refuses
one outright. Neither is touched, and `byte[]` is in every provider's memo and never reaches the
branch.

### What changes for you

- **`ToParameterType(typeof(int[]))` now throws on those four providers** rather than returning a
  character type, which is what `ToParameterType` has meant for any unmapped type since 9.47.1. Use
  `TryGetDbType` if you want the `null`.
- **A bound array value is no longer typed by Weasel.** If you were relying on the old behaviour to
  send an array as a delimited string, type the parameter yourself.

## `DateOnly` and `TimeOnly` on MySQL and Oracle

[#763](https://github.com/JasperFx/weasel/issues/763) /
[#772](https://github.com/JasperFx/weasel/pull/772). The two providers 9.47.1 left alone, and they
did not close the same way.

**MySQL maps both.** Verified by round trip against MySQL 8.0 rather than taken from
MySqlConnector's inference:

| CLR type | parameter type | column type | stored |
| --- | --- | --- | --- |
| `DateOnly` | `MySqlDbType.Date` | `DATE` | `2026-10-01` |
| `TimeOnly` | `MySqlDbType.Time` | `TIME` | `13:45:30` |

**Oracle cannot bind either, at all.** Measured against ODP.NET 23.1.0.0, with `OracleDbType`
assigned before the value:

| binding | result |
| --- | --- |
| `DateOnly` as `Date` | `InvalidCastException` — ODP.NET binds a date through `Convert.ToDateTime`, and `DateOnly` is not `IConvertible` |
| `DateOnly` or `TimeOnly` as `TimeStamp` | `ORA-50028: Invalid parameter binding` |
| `TimeOnly` as `IntervalDS` | `ArgumentException: Value does not fall within the expected range` |
| `TimeOnly` as `Varchar2` | executes, and writes the current culture's `ToString()` — `1:45 PM` |

Deferring is not available either: an **untyped** `OracleParameter` rejects the value outright, so
there is nothing for the driver to infer from. So Oracle refuses with a message that names the
conversion to make:

```
Oracle cannot bind a DateOnly parameter: ODP.NET has no conversion for it, and an untyped
parameter rejects the value before the driver sees it. Convert the value first -- a DateOnly
with ToDateTime(TimeOnly.MinValue) against a DATE column, a TimeOnly with ToTimeSpan() against
an INTERVAL DAY TO SECOND column. Weasel generates both column types for you. See weasel#763.
```

Oracle does gain both **column** types (`DATE` and `INTERVAL DAY TO SECOND`), which is the half of
the gap that can be closed: a schema can declare the column, and the value binds as the `DateTime`
or `TimeSpan` it converts to.

### What changes for you

- **Entity Framework Core is unaffected.** `Oracle.EntityFrameworkCore` applies its own value
  converter for both types, so an EF model mapping a `DateOnly` property keeps working. Verified, not
  assumed.
- **On MySQL, `GetDatabaseType` now answers for both**, where it previously threw.
- **On Oracle, a `DateOnly` parameter now throws a legible `NotSupportedException`** where it
  previously produced an `ArgumentException` from inside ODP.NET, or — before 9.47.1 — silently sent
  a culture-dependent string.

## SQL Server: a partition scheme that is still in use is no longer dropped

[#765](https://github.com/JasperFx/weasel/issues/765) /
[#771](https://github.com/JasperFx/weasel/pull/771).

A partition function and scheme are database-scoped objects with no `CREATE OR ALTER`, so the
creation DDL refreshed them by dropping first — unconditionally. That made the generated creation
script for **any** partitioned shape single-use. The second execution failed on the table the script
itself had created:

```
The partition scheme "ps_metrics_bucket_end" is currently being used to partition one or more tables.
```

Every other shape Weasel generates is re-runnable, which is what Polecat's
`generated_schema_script_executes` relies on; partitioned shapes had to be excused from that check
([polecat#842](https://github.com/JasperFx/polecat/issues/842)).

Two guards now, and both are needed — they have to agree, or the failure just moves one error over to
`There is already an object named ...`:

- The **drop** is conditional on nothing being partitioned on the scheme.
- The **creates** are conditional on the objects being absent.

What this deliberately does not do is reshape a scheme already bound to a table. Changing boundaries
on a live partitioned table is `SPLIT RANGE` / `MERGE RANGE` work, and that is what the delta path is
for; the creation script's job is to converge. A test pins that the table is still partitioned, with
the right number of partitions, after a second run — so "idempotent" cannot be satisfied by quietly
doing nothing.

### What changes for you

- **Nothing, unless you re-run a generated creation script for a partitioned table** — in which case
  it now succeeds instead of failing. The first run emits the same objects it always did.
- `WriteDropDdl` keeps its unconditional drop. An explicit request to remove the partition objects
  should say so when they are still in use rather than silently leaving them behind.

## SQL Server: partition boundaries no longer depend on the host's culture

[#764](https://github.com/JasperFx/weasel/issues/764) /
[#770](https://github.com/JasperFx/weasel/pull/770).

The partition DDL Weasel generates is SQL, not display text, but every arm of
`RangePartitioning.FormatSqlValue` rendered with the current culture. The schema a host emitted
therefore depended on where it ran. Found while fixing
[polecat#848](https://github.com/JasperFx/polecat/issues/848): Polecat's Storage and Schema suites
pass 21 of 21 on the default culture and lose 11 to partition tests under `--culture th-TH`.

Three distinct failures, all measured:

| culture | `new DateTime(2026,10,1,12,0,0).ToString("yyyy-MM-dd HH:mm:ss")` | `(-1.5m).ToString()` | `(-7).ToString()` |
| --- | --- | --- | --- |
| invariant | `2026-10-01 12:00:00` | `-1.5` | `-7` |
| `th-TH` | `2569-10-01 12:00:00` | `-1.5` | `-7` |
| `fi-FI` | `2026-10-01 12.00.00` | `−1,5` | `−7` |
| `de-DE` | `2026-10-01 12:00:00` | `-1,5` | `-7` |
| `sv-SE` | `2026-10-01 12:00:00` | `−1,5` | `−7` |

- **`th-TH`** applies the Buddhist calendar to a custom date format — a boundary 543 years from the
  one intended, and a host whose schema disagrees with one running invariant.
- **`fi-FI`** renders the `:` in `HH:mm:ss` as `.`, because `:` in a custom format string is the
  *time-separator placeholder* and not a literal colon. So the date arm was culture-dependent even
  where the calendar is Gregorian.
- **`de-DE`** gives a decimal a comma separator, which is an argument separator in T-SQL.
  **`sv-SE`**, `fi-FI`, `lt-LT` and `nb-NO` write the negative sign as U+2212 MINUS SIGN — and that
  applies to a plain `int`, not only to decimals.

The sweep turned up three more sites rendering into DDL with the current culture:
`Table.DefaultValue(int)` and `DefaultValue(long)` (the `double` overload was already invariant,
which is what makes the other two read as deliberate), the tenant ordinals `ManagedTenantPartitions`
writes into partition-function DDL, and — one provider over — **PostgreSQL's numeric arm** in
`PartitionExtensions.FormatSqlValue`, whose date arms were already invariant. That last one is the
same defect under the same cultures, so it is fixed here rather than left behind as a known twin.

### What changes for you

- **If any host that runs migrations is not on an invariant culture, upgrade.** On an invariant
  culture nothing changes: every rendering is byte-identical to what it was.
- **A boundary already written under a hostile culture is still wrong in the database.** This fixes
  what Weasel emits; it cannot correct a `2569` boundary already committed to a partition function.

## SQL Server: an `sp_getapplock` wait outlasts the command timeout

[#766](https://github.com/JasperFx/weasel/issues/766) /
[#768](https://github.com/JasperFx/weasel/pull/768).

`SharedLockExtensions.tryGetLock` passed the caller's wait to `sp_getapplock` as `@LockTimeout`, but
ran the command under the connection's `CommandTimeout` — 30 seconds unless the connection string
says otherwise. So the client gave up before the server answered:

- A requested wait of **30 seconds or more silently had no effect**.
- At exactly 30 seconds the client usually won the race, so a busy lock threw a raw `SqlException`
  (`Execution Timeout Expired`) instead of returning a failed `AttainLockResult`.
  `ResourceMigrationFailureMode.ContinueOnFailures` can never engage on an exception, and the message
  does not say what was being waited for.

Found from [polecat#868](https://github.com/JasperFx/polecat/issues/868), where `SqlServerGlobalLock`
against a held lock threw at 30.0s whether the lock timeout was 30s or 45s.

The command timeout is now sized from the lock wait:

| current `CommandTimeout` | lock wait | command timeout used |
| --- | --- | --- |
| 30 | 1000 ms | 30 — the default already outlasts it |
| 30 | 30 s | 35 — the boundary the report hit |
| 30 | 45 s | 50 |
| 120 | 1000 ms | 120 — a deliberate setting is never narrowed |
| 30 | `-1` (wait forever) | 0 — infinite, since no finite timeout can outlast it |

### What changes for you

- **If you set a migration lock timeout of 30s or more, it now takes effect.** Before, the client
  aborted at 30s regardless.
- **A busy lock is now a failed result rather than an exception**, so `ContinueOnFailures` rules on it
  as it already did on PostgreSQL.
- **Polecat's workaround can come out.** Catching error number `-2` around the lock call and
  reporting it as a lost lock is no longer needed, and the 30s cap it could not address is gone.

## Documented: strings on SQL Server are bound as non-Unicode, on purpose

[#762](https://github.com/JasperFx/weasel/issues/762) /
[#773](https://github.com/JasperFx/weasel/pull/773). **No product change** — this is here so that
nobody "fixes" it.

`Weasel.SqlServer` binds a `string` as `SqlDbType.VarChar`; the generic `DbCommandBuilder` binds it
as `DbType.String`, which SqlClient sends as `nvarchar`. `nvarchar` outranks `varchar` in SQL
Server's data type precedence, so the direction decides which side of a comparison is converted.
Measured on SQL Server 2025 from `sys.dm_exec_query_plan`, against an indexed column of 5,000 rows:

| parameter | column | plan |
| --- | --- | --- |
| `VarChar` | `varchar(100)` | Index Seek |
| `VarChar` | `nvarchar(100)` | Index Seek, the convert on the **parameter** |
| `NVarChar` | `nvarchar(100)` | Index Seek |
| `NVarChar` | `varchar(100)` | **Index Scan**, `CONVERT_IMPLICIT(nvarchar(100), [col])` on the **column** |

`varchar` is the safe direction — it seeks against either column shape. Both defaults are kept:
`DbType.AnsiString` on the generic provider would be seek-safe and would silently truncate
non-Latin1 text, and an `nvarchar` generated column type would be a schema diff and an `ALTER` for
every existing column downstream.

If you bind strings through the generic `DbCommandBuilder` against SQL Server and your columns are
`varchar`, pass the type explicitly: `builder.AppendParameter(value, DbType.AnsiString)`.

A `char` is the deliberate exception — `SqlDbType.NChar` against an `nchar(1)` column. It had no
mapping at all before 9.47.1, so there was no behaviour to preserve, the parameter and the column
agree with each other, and an `nchar` cannot lose a character.

## Also in this release

- **JasperFx and JasperFx.Events move to 2.98.0**
  ([#769](https://github.com/JasperFx/weasel/pull/769)), from 2.90.2.
- **The NuGet publish workflow uses NuGet.org trusted publishing** (OIDC) instead of a long-lived API
  key secret. No behaviour change in the packages.

## New public API

| Member | Notes |
| --- | --- |
| `IDatabaseProvider<,,>.ToParameterType(Type, EnumStorage)` | defaulted on the interface |
| `IDatabaseProvider<,,>.TryGetDbType(Type?, EnumStorage)` | defaulted on the interface |
| `IDatabaseProvider<,,>.TryGetDbTypeForValue(object, EnumStorage)` | defaulted on the interface |
| `Weasel.Core.EnumParameterType.Substitute(Type, EnumStorage)` | the shared rule the above are built on |
| `SharedLockExtensions.CommandTimeoutForLockWait` | `internal`; listed because its behaviour is observable |
| `OracleProvider.DateOnlyAndTimeOnlyRefusal(Type)` | the refusal message, public so it can be asserted |
