# Upgrading to 9.40

9.40 fixes a shutdown that could stall a host for two minutes in silence, and makes a generated
creation script runnable without the caller having to get its own yield order right. It also adds a
way for a query builder to ask how much of the provider's per-command parameter budget is already
spent.

**Nothing in this release is a breaking change.** Every addition is additive, nothing public was
removed, and the one new interface member carries a default implementation — see
[the note below](#icommandbuilder-parametercount) for why that detail is load-bearing rather than a
formality.

## ⚠️ A host stop no longer waits out the advisory lock

[#678](https://github.com/JasperFx/weasel/pull/678), from
[#676](https://github.com/JasperFx/weasel/issues/676).

`Weasel.Postgresql.AdvisoryLock.DisposeAsync` released its held locks one at a time, with no bound
and no logging. With session-scoped multiplexed locks *and* lock monitoring — which is what
`HasLock` activates — a host stop could take **60 or 120 seconds with nothing logged at all**.

The cause is not in Weasel. In `Patched.DistributedLock.Core` 1.0.11, a lock handle drops its
monitoring registration *before* running its `pg_advisory_unlock`, and
`ConnectionMonitor.AcquireConnectionLockAsync` only fires its wake-up while monitoring handles are
still registered. So the last release on a connection finds the count already at zero, fires
nothing, and sits in its own two-second retry loop re-evaluating that same false condition until the
monitor's one-minute window expires on its own. Two such connections stagger into the 120-second
case. There is no newer package to take.

Two things changed:

- **The releases run concurrently**, so a stop pays at most one stalled window rather than one per
  connection.
- **The wait is bounded** by the new `AdvisoryLockOptions.ReleaseTimeout`, default 5 seconds.

::: warning This changes what `DisposeAsync` waits for
`DisposeAsync` previously returned only once every release had completed. It now returns after
`ReleaseTimeout`, leaving any outstanding releases running in the background and logging a warning
naming how many.

Nothing leaks: the unlock still goes through when the monitor's window ends, and a process that is
exiting closes its connections and drops the session-scoped locks regardless. What a bound costs is
that leadership of those locks cannot move to another node until the releases finish — which is
strictly better than the old behaviour, where the *stop itself* waited that long.

If you need the old semantics, set `ReleaseTimeout` to `Timeout.InfiniteTimeSpan` or any
non-positive value. Both opt out of the bound and wait for every release, matching how JasperFx
reads `DaemonSettings.StopAndDrainTimeout`, which is what a daemon host feeds this from.
:::

`ReleaseTimeout` is a settable property rather than a constructor parameter on purpose — an optional
parameter on the existing four-argument constructor would be a binary break for anything already
compiled against Weasel, which is exactly what [9.33.0 shipped](/release-9-34) and 9.34.0 had to
repair.

`Weasel.SqlServer.AdvisoryLock` needed no equivalent change. It holds one dedicated connection and
calls `ReleaseGlobalLock` directly, with no multiplexing or connection monitor involved.

## A creation script is written in dependency order

[#680](https://github.com/JasperFx/weasel/pull/680), from
[#677](https://github.com/JasperFx/weasel/issues/677).

The migration path has resolved foreign-key ordering for the caller for a long time:
`SchemaMigration` finds every key pointing at a table created later in the same migration and defers
it. The **script** path had no equivalent — `WriteFeatureCreation` wrote
`IFeatureSchema.Objects` in yield order, verbatim. So the same model could migrate cleanly and
produce a creation script that failed on its first constraint:

```
-- PostgreSQL
42P01: relation "myschema.parent" does not exist

-- SQL Server
Foreign key 'fk_child_to_parent' references invalid table 'myschema.parent'
```

Yield order was load-bearing in one path and not the other, and nothing said so. Consumers were left
to re-derive the ordering themselves.

Both `ToDatabaseScript()` and `WriteScriptsByTypeAsync()` now order objects **and** features so that
a referenced object is created first. Ordering features matters as well as ordering within one:
`WriteScriptsByTypeAsync` puts every feature in its own file, so there the feature order *is* the
script order, and nothing inside a feature can reach across. Feature edges are taken from where the
objects actually point, not only from the declared `IFeatureSchema.DependentTypes()` — that
collection is documented as controlling "the proper ordering of object creation or scripting" but
was only ever read by the lazy per-feature storage path, so a consumer who never declared it had
nothing but dictionary order. Declared types are honoured too.

The sort is stable, so a script's shape only changes where it had to, and a self-referencing foreign
key is not treated as an ordering constraint.

A table's foreign keys need no opt-in. For a dependency that is **not** a foreign key — a trigger
whose body writes to another table, a view selecting from one — implement the new
`ISchemaObjectWithDependencies`:

```csharp
public interface ISchemaObjectWithDependencies : ISchemaObject
{
    IEnumerable<DbObjectName> DependsOn { get; }
}
```

A name the script does not create is ignored, so declaring a dependency on something external is
harmless.

### A mutual reference still cannot be scripted

Two tables referencing each other have no valid creation order, so there is nothing to sort them
into. The sort leaves them where it found them rather than refusing: refusing would reject a
configuration the **migration** path handles fine by deferring one of the keys, and on SQLite even
that is impossible — it has no `ALTER TABLE … ADD CONSTRAINT`, so a foreign key can only be declared
inline at table creation.

So for a cycle, apply the migration rather than running a script. That is not new, but it is now a
decision on record instead of a surprise.

### SQLite was never affected, and that is worth knowing

Measured both ways. SQLite resolves a foreign key's target by name at DML time rather than at
`CREATE TABLE`, so a child declared before its parent runs clean **and links correctly** — verified
by inserting rows with `PRAGMA foreign_keys = ON`. Only PostgreSQL and SQL Server ever failed.

The SQLite ordering still changed, because a script that is correct only because SQLite is forgiving
is not much use to any other tool reading it.

## `ICommandBuilder.ParameterCount` {#icommandbuilder-parametercount}

[#679](https://github.com/JasperFx/weasel/pull/679), from
[#675](https://github.com/JasperFx/weasel/issues/675).

A consumer rendering a value list had no way to ask how much of the provider's per-command parameter
budget was already spent, so it had to guess conservatively or ignore
`Migrator.MaxParametersPerCommand` entirely. The budget belongs to the **command**, not to the
fragment, which is why a per-fragment threshold does not close the hole:

```csharp
.Where(x => a.Contains(x.Id) && b.Contains(x.Status))   // 1,500 values each
```

Both fragments are far under any sane per-fragment limit; together they are past SQL Server's 2,100.
The second one had no way to know the first had spent 1,500 slots.

`ICommandBuilder.ParameterCount` answers for **the command currently being built**, which is the
only count comparable against a per-command limit. On the builders that spread work over several
commands — the PostgreSQL and SQL Server `BatchBuilder`s, and the Oracle and Firebird
`DbCommandBuilder`s, whose drivers execute one statement per command — it counts from the command
being filled now and resets at each `StartNewCommand`. On the providers that concatenate into one
command, `StartNewCommand` is a no-op and the count keeps rising, which is correct: there is only
ever one command to compare against the limit.

Read it rather than parsing an index back out of `LastParameterName`. That assumes the naming scheme
stays positional, and it is simply wrong for parameters added through `AddParameters(IDictionary<,>)`
or `AppendWithParameters`, neither of which goes through `ParameterNames.ForPosition`.

### If you implement `ICommandBuilder` yourself

**You do not have to change anything.** The member has a default implementation returning
`ICommandBuilder.UnknownParameterCount` (`-1`), so a builder compiled against an earlier Weasel keeps
working — including one already published, loaded against this release by NuGet resolving Weasel up
on its own.

That default is not a formality. Without it, adding this member is a **runtime** break rather than a
compile-time one: the CLR refuses the type the first time a host loads it, and no amount of building
or restoring reveals it beforehand. It is the same category as the [9.33.0 binary
break](/release-9-34), one level deeper.

Adopt it when you want callers to get a real count:

```csharp
public int ParameterCount => _command.Parameters.Count;
```

Negative is the "no information" answer, not a count. A caller **must** treat it as such and fall
back to whatever conservative path it took before this member existed — which is why the sentinel is
negative rather than `0`. Zero is a legitimate count, and a stale builder reporting it would tell a
caller the command's whole budget was still free, the dangerous direction to be wrong in for a member
that exists to keep callers under a limit.

## Known issue

[#681](https://github.com/JasperFx/weasel/issues/681) — on PostgreSQL, a generated creation script
for a model with foreign keys **cannot be re-run**. The table creations carry `IF NOT EXISTS` guards
but the trailing `ALTER TABLE … ADD CONSTRAINT` does not, so a second run reaches it and fails:

```
42710: constraint "fk_child_to_parent" for relation "child" already exists
```

This predates 9.40 and is not caused by the ordering change above; it is simply what that change made
easy to notice. A model with no foreign keys re-runs fine, as does the migration path in every case.

## New public API

- `Weasel.Postgresql.AdvisoryLockOptions.ReleaseTimeout` — the bound on a shutdown release, described
  above.
- `Weasel.Core.ICommandBuilder.ParameterCount` and `ICommandBuilder.UnknownParameterCount`, described
  above. Defaulted, so existing implementations are unaffected.
- `Weasel.Core.Migrations.ISchemaObjectWithDependencies` — declare a creation-order dependency that is
  not a foreign key.
- `Weasel.Core.Migrations.SchemaObjectOrdering` — `InDependencyOrder` for both `ISchemaObject` and
  `IFeatureSchema` collections, should you want the same ordering for a script you assemble yourself.
