# Migrating from 9.x to 10.0

Weasel 10.0 ships with the **Critter Stack .NET 11 release train**: JasperFx 3.0, Weasel 10.0, Marten 10.0,
Polecat 6.0, Fisher 2.0, Wolverine 7.0 and CritterWatch 2.0 all move to `net10.0;net11.0` together. For Weasel
itself there is no large API rewrite; the work is the target framework floor, newer driver majors, one
newly shipped storage family, and one obsolete method.

## At a glance

| Theme | Effort |
| --- | --- |
| `net9.0` dropped; targets are `net10.0` and `net11.0` | Move your application to `net10.0` or later |
| `JasperFx` / `JasperFx.Events` 3.0 | Pin bump (move the rest of the Critter Stack with it) |
| `Npgsql` 10, `Microsoft.Data.SqlClient` 7, `Oracle.ManagedDataAccess.Core` 23.26 | Pin bump; read the driver release notes |
| EF Core 10 is the floor for `Weasel.EntityFrameworkCore` | Pin bump; EF Core 10 also runs on the .NET 11 runtime |
| `Weasel.Sqlite` stays on the `e_sqlite3` bundle, SQLitePCLRaw 3.0.x | None, unless you choose a different bundle |
| `Weasel.Storage` closed-shape document storage family | Additive; only matters if you build a document store on Weasel |
| `ConnectionSourceExtensions.RunSql<T>` is `[Obsolete]` | Switch to `RunSqlAsync<T>` |
| MySQL tests for the EF Core bridge are compiled out | No library change; Pomelo has no EF Core 10 release |

## Target frameworks

Weasel 10.0 drops `net9.0`. Every package targets `net10.0;net11.0`.

```xml
<!-- before (Weasel 9.x) -->
<TargetFrameworks>net9.0;net10.0</TargetFrameworks>

<!-- after (Weasel 10.0) -->
<TargetFrameworks>net10.0;net11.0</TargetFrameworks>
```

An application still on `net9.0` cannot reference Weasel 10.0; stay on the 9.x line until you can move.
The `net11.0` build of JasperFx 3.0 depends on the .NET 11 hosting packages, so an application targeting
`net11.0` resolves the matching `Microsoft.Extensions.Hosting` and `Microsoft.Extensions.Logging.Abstractions`
versions transitively. If you pin those yourself, do not pin them below what JasperFx asks for, or restore
fails with `NU1605` (package downgrade).

## Package pins

| Package | 9.x | 10.0 |
| --- | --- | --- |
| `JasperFx`, `JasperFx.Events`, `JasperFx.SourceGenerator` | `2.x` | `3.0.0` |
| `Npgsql` | `9.0.x` | `10.0.x` |
| `Npgsql.NetTopologySuite` | `9.0.x` | `10.0.x` |
| `Microsoft.Data.SqlClient` | `6.1.x` | `7.1.x` |
| `Oracle.ManagedDataAccess.Core` | `23.7.x` | `23.26.x` |
| `Microsoft.EntityFrameworkCore.Relational` / `.SqlServer` | `9.x` | `10.0.0` |
| `Npgsql.EntityFrameworkCore.PostgreSQL` | `9.x` | `10.0.0` |
| `Oracle.EntityFrameworkCore` | not part of the EF Core bridge's suite on `net9.0` | `10.23.x` |

Weasel's packages are versioned together: bump every `Weasel.*` package your application references in one
change, and do not mix Weasel 10.0 with the Marten, Polecat, Fisher or Wolverine majors of the previous wave.

### Npgsql 10

`Weasel.Postgresql` is built and tested against Npgsql 10. If your application pins Npgsql itself, pin 10.x;
Npgsql 9 and Weasel 10 are not a tested pairing. See the
[Npgsql 10 release notes](https://www.npgsql.org/doc/release-notes/10.0.html) for the driver's own breaking
changes.

### Microsoft.Data.SqlClient 7

`Weasel.SqlServer` moves from SqlClient 6 to 7. Weasel's own SQL Server surface is unchanged, but connection
and encryption defaults belong to the driver, so read the SqlClient 7 release notes if your connection strings
rely on a default. `Weasel.SqlServer` and `Weasel.Oracle` also reference
`System.Security.Cryptography.Pkcs` directly, at a patched floor, because the drivers' own transitive
references are on a vulnerable version (GHSA-846f-f764-76mx). That reference is deliberate; leave it in place.

### Oracle

`Oracle.ManagedDataAccess.Core` moves to 23.26. With `net9.0` gone, Oracle builds on the same two frameworks as
every other provider and the `Weasel.Oracle` package no longer carries a per-framework reference split.
`Oracle.EntityFrameworkCore` 10 is now supported by the EF Core bridge, and its tests run in the gate.

### EF Core 10 as the floor

`Weasel.EntityFrameworkCore` references EF Core 10 on both `net10.0` and `net11.0`. A stable package cannot
depend on an EF Core 11 prerelease, and EF Core 10 runs on the .NET 11 runtime. An application on EF Core 9
needs to move to EF Core 10 first.

`Pomelo.EntityFrameworkCore.MySql` has no EF Core 10 release (its latest is `9.0.0-preview.3`), so the
MySQL half of the EF Core bridge's test suite is compiled out. `Weasel.MySql` itself, on `MySqlConnector`, is
unaffected.

## SQLite: the native bundle

`Weasel.Sqlite` references `Microsoft.Data.Sqlite` plus `SQLitePCLRaw.bundle_e_sqlite3` on the SQLitePCLRaw
3.0.x line (currently 3.0.5). `Microsoft.Data.Sqlite` 11 depends on that same bundle and version, so moving to
it needs no change. Weasel does not swap in `SQLite3MC.PCLRaw` (an encrypted build, which is a choice for an
application to make) and does not cap `Microsoft.Data.Sqlite` below 11. An application that wants SQLite3MC can
reference its provider package itself. See [SQLite](/sqlite/) for details.

## `RunSql<T>` is obsolete

`ConnectionSourceExtensions.RunSql<T>(this IConnectionSource<T>, string)` blocks a thread on I/O and is now
marked `[Obsolete]`. It still works in 10.0 and will be removed in a later major version. Use the async
overload:

```csharp
// before
source.RunSql("create table ...");

// after
await source.RunSqlAsync("create table ...");
```

Weasel's own build suppresses obsolete warnings (`NoWarn` includes `0618`); your application does not, so
expect a compiler warning at each call site until you switch.

## `Weasel.Storage`: the closed-shape document storage family

`Weasel.Storage` gains the **closed-shape** document storage family, relocated from Marten so Marten, Polecat
and Fisher share one implementation:

- a `ClosedShapeDocumentStorage` base with four tracking modes (query-only, lightweight, identity map and
  dirty tracking),
- the concrete leaves for flat and hierarchical documents under unversioned, numeric-revision and
  optimistic-version concurrency,
- the insert, update, upsert and overwrite operations, the document metadata binders, and the
  `ClosedShapeDocumentProviderFactory` and `ProviderGraphBase` that build them.

This is additive: nothing already in `Weasel.Storage` changed shape, and an application that only uses
`Weasel.Postgresql`, `Weasel.SqlServer` and the other schema providers does not touch it. It matters to you if
you maintain a document or event store on Weasel, in which case:

- Construct storage through the **non-generic** `ClosedShapeDocumentProviderFactory`. The generic
  instantiations need value-type type arguments, which `MakeGenericType` cannot close under Native AOT.
- A closed-shape identity map refuses an id whose type does not match the document's identity with
  `IdentityMapIdTypeMismatchException`.
- The package is `IsAotCompatible`. As always, publish natively and run the result before believing an AOT
  claim; the analyzer cannot prove the runtime half.

## Behavior and build changes that need no code

- Several static regular expressions are now source-generated (internal performance work).
- The SQL Server table introspection query reuses one compiled plan.
- The Nuke build host targets `net10.0`. `./build.sh Pack`, `AotSmoke` and `LocalGate` are the local-train
  entry points; see `CLAUDE.md` in the repository.

## Dependency lockstep

| Weasel | JasperFx | JasperFx.Events | Marten | Polecat | Fisher | Wolverine |
| --- | --- | --- | --- | --- | --- | --- |
| 10.0 | 3.0 | 3.0 | 10.0 | 6.0 | 2.0 | 7.0 |

Mixing majors across products is unsupported in this wave.

## References

- [Migrating from 8.x to 9.0](/migration-guide)
- [Provider Trait Matrix](/core/provider-trait-matrix)
- [Weasel 10.0 migration guide tracking issue](https://github.com/JasperFx/weasel/issues/746)
