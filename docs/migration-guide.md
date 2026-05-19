# Migrating from 8.x to 9.0

Weasel 9.0 ships in lockstep with the [Critter Stack 2026](https://github.com/JasperFx/jasperfx/issues/217) release wave — [Marten 9.0](https://martendb.io), [JasperFx 2.0](https://github.com/JasperFx/jasperfx/issues/215), [JasperFx.Events 2.0](https://github.com/JasperFx/jasperfx/issues/215), and [Polecat 4.0](https://github.com/JasperFx/polecat/issues/46) all consume the same 9.0 alpha line. 9.0 is the consolidation cycle for Weasel: there are no large rewrites, but the cross-provider audit at [#270](https://github.com/JasperFx/weasel/issues/270) extracted significant shared infrastructure into `Weasel.Core` and tightened several APIs. Most upgrades are pin bumps plus a small `using` directive change; the breaking surface is small and localized.

::: tip Provider feature matrix
Once you've migrated, the [Provider Trait Matrix](/core/provider-trait-matrix) is the canonical reference for what works on every provider versus what's provider-specific.
:::

## At a glance

| Theme | Effort |
| --- | --- |
| Pin bumps + drop `net8.0` target | Renovate / Dependabot |
| `Weasel.Postgresql.CascadeAction` / `Weasel.SqlServer.CascadeAction` → `Weasel.Core.CascadeAction` | Add `using Weasel.Core;` + pass enum values explicitly |
| `Weasel.*` `MisconfiguredForeignKeyException` → `Weasel.Core` | Add `using Weasel.Core;` |
| `BulkInsertMode` consolidated into `Weasel.Core` | Add `using Weasel.Core;` |
| Identity fluent rename: `Serial()` / `AutoNumber()` → `AutoIncrement()` | Find/replace, old names obsolete one cycle |
| `SchemaObjectDelta<T>.WriteRestorationOfPreviousState` is now `virtual` | Subclasses that need the no-op-on-null-Actual semantic override it |
| SQL Server `TableColumn` casing now preserved | Schema regenerates on next migration; no caller change |
| SQLite composite primary keys emit as table-level constraint | DDL change only; no caller change |
| `IsAotCompatible=true` on all Weasel assemblies | Consumers can publish AOT (see [Publishing AOT](https://jasperfx.github.io/jasperfx/codegen/aot)) |

## Foundation pin bumps

Weasel 9.0 consumes the JasperFx 2.0 alpha line:

| Package | 8.x | 9.0 (current alpha) |
| --- | --- | --- |
| `JasperFx` | `1.x` | `2.0.0-alpha.x` |
| `JasperFx.Events` | `1.x` | `2.0.0-alpha.x` |
| `JasperFx.CommandLine` (now folded into `JasperFx`) | `1.x` | — |
| `Microsoft.Data.SqlClient` (SqlServer flavor) | `5.x` | `6.0.x` |
| `Npgsql` (Postgresql flavor) | `8.x` | unchanged |

The alpha line is still rolling forward as the 2026 wave converges; consult the [9.0 NuGet listings](https://www.nuget.org/packages?q=Weasel.) for the latest pin and bump all the `Weasel.*` packages your application references together.

### Target framework changes

Weasel 9.0 drops `net8.0`. Supported target frameworks are `net9.0` and `net10.0`.

```xml
<!-- before (Weasel 8.x) -->
<TargetFrameworks>net8.0;net9.0</TargetFrameworks>

<!-- after (Weasel 9.0) -->
<TargetFrameworks>net9.0;net10.0</TargetFrameworks>
```

If you're not ready to drop `net8.0` from your own application, stay on the 8.x line — Weasel 8.x continues to receive critical fixes until the 2026 wave is fully GA.

## Dedup audit relocations

The Critter Stack dedup audit ([jasperfx#218](https://github.com/JasperFx/jasperfx/issues/218) plus weasel#264) consolidated several enums and exceptions that had parallel definitions in `Weasel.Postgresql`, `Weasel.SqlServer`, and downstream consumers (Marten, Polecat) into canonical homes in `Weasel.Core`. The relocations are mechanical — same values, different namespace.

### `Weasel.Postgresql.CascadeAction` / `Weasel.SqlServer.CascadeAction` → `Weasel.Core.CascadeAction`

The provider-local `CascadeAction` enums on PostgreSQL and SQL Server were marked `[Obsolete]` and are scheduled for removal in a future major. The canonical type is `Weasel.Core.CascadeAction`, which Oracle and MySQL have already been using.

`ColumnExpression.ForeignKeyTo` (and `Table.ForeignKey`) gained sibling overloads accepting `Weasel.Core.CascadeAction`. The new overloads intentionally have **no default values** to avoid call-site ambiguity with the existing obsolete overloads — callers opt into the canonical enum by passing the action values explicitly:

```csharp
// before (Weasel 8.x — obsolete enum)
using Weasel.Postgresql.Tables;

table.AddColumn<int>("user_id")
     .ForeignKeyTo("users", "id",
                   onDelete: CascadeAction.Cascade);
//                            ^ resolved Weasel.Postgresql.CascadeAction

// after (Weasel 9.0 — canonical)
using Weasel.Core;
using Weasel.Postgresql.Tables;

table.AddColumn<int>("user_id")
     .ForeignKeyTo("users", "id",
                   fkName: null,
                   onDelete: CascadeAction.Cascade,
                   onUpdate: CascadeAction.NoAction);
//                            ^ resolves Weasel.Core.CascadeAction
```

Defaults will return to the canonical overload once the local enums are removed in 10.0.

### `Polecat.BulkInsertMode` → `Weasel.Core.BulkInsertMode`

[#50 (Polecat)](https://github.com/JasperFx/polecat/pull/50), [weasel#264](https://github.com/JasperFx/weasel/issues/264). The bulk-insert mode enum that Polecat 3.x defined locally is now in `Weasel.Core` and gained a fourth value, `OverwriteIfVersionMatches`. Third-party consumers that referenced `Polecat.BulkInsertMode` need to update their `using` directive:

```csharp
// before (Polecat 3.x)
using Polecat;
await store.Advanced.BulkInsertAsync(docs, BulkInsertMode.OverwriteExisting);

// after (Polecat 4.0 / Weasel 9.0)
using Weasel.Core;
await store.Advanced.BulkInsertAsync(docs, BulkInsertMode.OverwriteExisting);
```

### `MisconfiguredForeignKeyException` consolidated in `Weasel.Core`

`MisconfiguredForeignKeyException` was defined identically and unused in four of the five providers. The canonical type now lives in `Weasel.Core`; the per-provider duplicates have been removed. Any consumer that referenced it via a provider namespace needs the `using` updated:

```csharp
// before (Weasel 8.x)
using Weasel.Postgresql; // or .SqlServer / .Oracle / .MySql

catch (MisconfiguredForeignKeyException ex) { ... }

// after (Weasel 9.0)
using Weasel.Core;

catch (MisconfiguredForeignKeyException ex) { ... }
```

The PG-specific `InvalidForeignKeyException` and `TryToCorrectForLink` extension stay where they were.

### `Weasel.Core.IAdvisoryLock` → `JasperFx.Events.Daemon.IAdvisoryLock`

[#284](https://github.com/JasperFx/weasel/issues/284). The advisory-lock contract was a byte-identical duplicate of the one upstream JasperFx.Events lifted into `JasperFx.Events.Daemon` (jasperfx#316 / alpha.19) so the daemon contracts have a single canonical home. Weasel's duplicate has been removed; `Weasel.Postgresql.AdvisoryLock` and `Weasel.SqlServer.AdvisoryLock` now implement the lifted interface directly, satisfying the `lockFactory` closure that the lifted `*ProjectionDistributor` concretes accept.

Update the `using` statement:

```csharp
// before (Weasel 8.x / 9.0.0-alpha.5 and earlier)
using Weasel.Core;

IAdvisoryLock lock = new Weasel.Postgresql.AdvisoryLock(...);

// after (Weasel 9.0.0-alpha.7+)
using JasperFx.Events.Daemon;

IAdvisoryLock lock = new Weasel.Postgresql.AdvisoryLock(...);
```

The interface shape is unchanged (same three members: `HasLock` / `TryAttainLockAsync` / `ReleaseLockAsync`, with `IAsyncDisposable`). The two `AdvisoryLock` concretes' constructors and behaviour are also unchanged.

## API changes

### Canonical `AutoIncrement()` fluent API

[#270 step 10](https://github.com/JasperFx/weasel/issues/270). Identity / auto-increment column declarations are now spelled the same way on every provider:

```csharp
// Same code, every provider:
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
```

The pre-9.0 provider-specific names remain available as `[Obsolete]` aliases for one major-version cycle so existing schema-building code compiles unchanged, with a gentle nudge in your build output:

| Provider | Canonical 9.0 | Pre-9.0 alias |
| --- | --- | --- |
| PostgreSQL | `AutoIncrement()` | `Serial()` |
| PostgreSQL | `BigAutoIncrement()` | `BigSerial()` |
| PostgreSQL | `SmallAutoIncrement()` | `SmallSerial()` |
| SQL Server | `AutoIncrement()` | `AutoNumber()` |
| Oracle | `AutoIncrement()` | `AutoNumber()` |
| MySQL | `AutoIncrement()` | `AutoNumber()` |
| SQLite | `AutoIncrement()` | (unchanged) |

The SQL emitted by each provider is unchanged from 8.x — only the C# spelling on the caller side is normalized. See the [Provider Trait Matrix](/core/provider-trait-matrix#identity-auto-increment) for the SQL each canonical call produces per provider.

### `SchemaObjectDelta<T>.WriteRestorationOfPreviousState` is now `virtual`

Before 9.0 this method was sealed and would throw a `NullReferenceException` on subclasses where `Actual` was legitimately `null` (no previous state to restore). SQLite's `ViewDelta` carried that case and was hand-rolling `ISchemaObjectDelta` directly to dodge the NRE.

In 9.0 the method is `virtual`, and SQLite `ViewDelta` now inherits from `SchemaObjectDelta<View>` like the other provider deltas. Subclasses that want the no-op-on-null-`Actual` semantic should override the method:

```csharp
protected override void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
{
    // No-op when there's no previous state captured.
    if (Actual is null) return;
    base.WriteRestorationOfPreviousState(rules, writer);
}
```

Existing subclasses that always have a non-null `Actual` work unchanged.

### `ForeignKey.Equals` widening

[#270 step 4](https://github.com/JasperFx/weasel/issues/270). The provider `ForeignKey` types previously did strict `GetType() == GetType()` equality. In 9.0 equality is widened to "same provider root" — `Equals(object?)` walks up the inheritance chain to find the immediate subclass of `ForeignKeyBase` (the provider's concrete `ForeignKey` class) and uses `IsAssignableFrom`.

This restores a pattern Marten and similar libraries rely on: consumer-supplied marker subclasses (e.g. `MartenForeignKey : Weasel.Postgresql.Tables.ForeignKey`) compare equal to plain `ForeignKey` instances of the same provider as they did before the 9.0 base-class refactor. Cross-provider comparison still returns `false`.

The `GetHashCode` implementation now omits the column-arrays from the hash — array-reference hashing in 8.x produced different hashes for structurally-equal foreign keys and broke `HashSet` / `Dictionary` semantics. The new implementation accepts a higher collision rate to preserve correctness; equality still considers the full column list.

### Behaviour fixes worth flagging

**SQL Server column casing.** [#272](https://github.com/JasperFx/weasel/pull/272). `TableColumn` names declared on a SQL Server table now preserve the casing you wrote in code. Pre-fix the column-name initialization silently lower-cased, breaking case-sensitive collations. No caller change is required — the schema regenerates on next migration. The fix was backported as Weasel 8.15.2 ahead of the 9.0 release, so you may already have it.

**SQLite composite primary keys.** Composite primary keys on SQLite tables are emitted as a table-level `PRIMARY KEY (col_a, col_b)` constraint instead of multiple per-column `PRIMARY KEY` declarations (which SQLite rejects at table-creation time). No caller change is required.

## New surface (no migration impact)

These are additions in 9.0 that existing 8.x code doesn't need to opt into, but you'll see them in stack traces and reflection-based code. Documented for completeness.

### Shared schema-object base classes

[#270 steps 1–9](https://github.com/JasperFx/weasel/issues/270) extracted the boilerplate that every provider's schema-object types were reimplementing into shared base classes in `Weasel.Core`. Each concrete provider type retains its provider-specific DDL hooks and only inherits the cross-cutting plumbing:

| Schema object | New base in `Weasel.Core` |
| --- | --- |
| Tables | `TableBase<TColumn, TIndex, TForeignKey>` |
| Foreign keys | `ForeignKeyBase` |
| Sequences | `SequenceBase` |
| Functions | `FunctionBase` |
| Views | `ViewBase` |
| All of the above | `SchemaObjectBase` |

The new bases own things like `AllNames`, `Identifier`, `FindDeltaAsync`, `WriteDropStatement`, `Equals` / `GetHashCode`, `ColumnFor` / `HasColumn` / `IndexFor` / `IgnoreIndex`, identifier comparison rules, default primary-key name generation, and the `ToBasicCreateTableSql()` template. See the [Provider Trait Matrix](/core/provider-trait-matrix) for the per-provider rules each base parameterises.

`IgnoredIndexes` is now uniform across all five providers (was PG- and SQLite-only in 8.x). Empty set on SQL Server / Oracle / MySQL is a no-op until those providers wire it through `WriteCreateStatement`.

### `IDdlSyntaxStrategy`

[#270 step 8](https://github.com/JasperFx/weasel/issues/270). The shared CREATE / DROP algorithm is gradually migrating to consume a per-provider `IDdlSyntaxStrategy` strategy object. The 9.0 prototype is wired up for PostgreSQL and SQLite — the two providers at opposite ends of the feature spectrum — and currently routes:

- `WriteDropTable` (PG appends `CASCADE`, SQLite doesn't)
- `WriteCreateTableHeader` (with / without `IF NOT EXISTS`)

The remainder of `WriteCreateStatement` is still per-provider while the strategy shape settles. Plan is to migrate column / PK / FK emission as a 9.x point release.

If you have third-party code that subclasses a provider's `Table`, the prototype is opt-in — you don't need to consume `IDdlSyntaxStrategy` to upgrade. Once the full algorithm migrates the strategy will become required for custom `Table` implementations and we'll document the upgrade path then.

### AOT / trimming posture

[jasperfx#213](https://github.com/JasperFx/jasperfx/issues/213) AOT pillar. All five Weasel provider assemblies plus `Weasel.Core` and `Weasel.CommandLine` set `IsAotCompatible=true` in 9.0. Reflective surfaces are annotated where the runtime contract is sound (`CommandBuilderBase.AddParameters` carries `[DynamicallyAccessedMembers(PublicProperties)]` on the `parameters` argument, for example) and `[RequiresDynamicCode]` is applied where the runtime requirement is genuine (the CLI `AssertCommand` uses `Spectre.Console.AnsiConsole.WriteException`, which needs dynamic code for its exception formatter).

For the end-to-end "how do I publish AOT against the Critter Stack" walkthrough — recommended csproj flags, `WarningsAsErrors=IL*` setup, smoke-test pattern — see the [Publishing AOT with JasperFx](https://jasperfx.github.io/jasperfx/codegen/aot) guide. The guide is written for the whole Critter Stack; per-package status (Weasel, JasperFx, JasperFx.Events, Marten, Polecat, Wolverine) is in the table at the end.

## Dependency lockstep

Weasel 9.0 ships in lockstep with the rest of the Critter Stack 2026 wave. The supported pairings are:

| Weasel | Marten | Polecat | JasperFx | JasperFx.Events |
| --- | --- | --- | --- | --- |
| 9.0 | 9.0 | 4.0 | 2.0 | 2.0 |

Mixing major versions across products is unsupported in this wave (the dedup work moves abstractions between assemblies and ABI-binds them to specific majors). If you upgrade Weasel to 9.0, plan to upgrade Marten / Polecat to their 9.0 / 4.0 lines at the same time.

## References

- [Critter Stack 2026 umbrella](https://github.com/JasperFx/jasperfx/issues/217)
- [Weasel 9.0 audit + refactor (#270)](https://github.com/JasperFx/weasel/issues/270)
- [Provider Trait Matrix](/core/provider-trait-matrix)
- [Marten 8 → 9 migration guide](https://martendb.io/migration-guide)
- [Polecat 3 → 4 migration guide](https://jasperfx.github.io/polecat/migration-guide)
- [JasperFx 2.0 master plan](https://github.com/JasperFx/jasperfx/issues/215)
