# Upgrading to 9.33

9.33 is the output of an exception sweep across the whole library: eight issues about what Weasel
*says* when something goes wrong, and one dependency bump. No migration behaves differently, no DDL
changes, and nothing needs a schema change to adopt. What changes is the exception you catch and the
text you read at three of the most common failures in a deployment.

::: warning Three failures change their exception type
A `catch` written against the old type stops matching. None of these is a compile break; all three
keep the original exception as `InnerException`.

| Failure | Was | Now |
|---|---|---|
| A role lacking privilege during a migration | the raw `PostgresException` / `SqlException` / `MySqlException` / `OracleException` | `InsufficientDatabasePrivilegeException` |
| `sp_getapplock` refusing the SQL Server migration lock | a bare `System.Exception` | `GlobalLockUnavailableException` (an `InvalidOperationException`) |
| `AssertDatabaseMatchesConfigurationAsync` on drift that cannot be applied incrementally | `SchemaMigrationException` | `DatabaseValidationException` |

If you match on a SQLSTATE — `catch (PostgresException e) when (e.SqlState == "42501")` is the
common one — see [below](#permission-failures-are-named-now).
:::

::: tip Coming from 9.32?
Everything else is additive. The new warning on `AutoCreate.All` and the new `resources check` text
appear without being asked for; the rest — `Migrator.RefuseDestructiveChanges`, the append
exception-transform hook, `ISchemaObjectDeltaWithReason` — is opt-in and defaults to what 9.32 did.
:::

## Permission failures are named now

[#598](https://github.com/JasperFx/weasel/issues/598).

From support traffic this is the single most common real-world migration failure, and until now the
only signal was a SQLSTATE in an inner exception, sitting next to DDL the migration logger had
already printed. Weasel translated exactly one permission failure anywhere — the MySQL
view-comparison case.

A refused migration statement now raises `InsufficientDatabasePrivilegeException`:

> The role 'app_writer' does not have permission to apply this schema change in database 'things'.
> The database said: 42501: permission denied for schema things
> Statement:
> create table things.foo ()
> Either grant the role the privileges it needs (CREATE on the schema, or ownership of the objects
> being altered), or pre-provision the schema by running the output of db-patch as a privileged user
> and running this application with AutoCreate.None.

`Role`, `Database` and `Statement` are properties, and the provider's own exception is the
`InnerException` — nothing is hidden, only named. Translation happens where the statement runs, so
it covers both schema creation and DDL against an object owned by somebody else, the second of which
lands mid-migration after earlier statements have already auto-committed.

The codes each provider treats as a permission refusal:

| Provider | Codes |
|---|---|
| PostgreSQL | `42501` |
| SQL Server | `229`, `230`, `262`, `297`, `300`, `15247`, scanned across the whole `SqlErrorCollection` |
| MySQL | `1044`, `1142`, `1143`, `1227`, `1370` |
| Oracle | `ORA-01031`, `ORA-01950` |

Two codes are deliberately **not** on that list. MySQL `1045` is a failed login, which never reaches
a migration statement. Oracle `ORA-00942` is "table or view does not exist", which Oracle uses for
both a missing object and an invisible one — calling a genuinely missing table a permission failure
would send the reader off to check grants that are fine, which is worse than the ambiguity it tries
to resolve. SQLite has no privilege model and opts out entirely.

### Introspection is privilege-filtered, and that looks like an empty database

The half of this that does not announce itself. Catalog reads — `information_schema`, `pg_*`,
`sys.*`, `all_*` — are filtered by the connection's privileges on every provider, so an object the
role cannot see is indistinguishable from one that is not there. A restricted role therefore makes
Weasel conclude that *every* object is missing: under `CreateOrUpdate` it tries to create them and
hits the failure above, and under `db-assert` it reports the entire configuration as absent, which
reads as "the database is empty" when it is not.

`AssertDatabaseMatchesConfigurationAsync` now says so, in the one shape that can mean it — every
delta that reported anything reported *missing*, and more than one of them. A partially-migrated
database is ordinary drift and gets nothing.

The docs gain a [Permissions](/core/schema-migrations#permissions) section, which they did not have.

## The migrator warns before it drops a table

[#600](https://github.com/JasperFx/weasel/issues/600).

When a delta cannot be expressed as an `ALTER` and cannot rebuild in place, the migrator answers it
by writing a `DROP` followed by a `CREATE`. For a table that is the table's data. It is the only
branch in the migrator that destroys anything, every `AutoCreate` except `All` is refused before
reaching it — and nothing at runtime said it was about to happen. The migration logger printed the
DDL as it executed, and that was the whole warning.

It now warns first, once per object, before any statement runs:

> AutoCreate.All is dropping and recreating things.documents because column 'name' cannot be added to
> an existing table; any rows in it will be lost. Use db-patch to see the migration, or
> AutoCreate.CreateOrUpdate to be refused instead.

This arrives through a new `IMigrationLogger.DestructiveChange(string)`, which has a default
implementation — so every existing logger gains the warning without being changed, and a logger that
wants it at Warning level in a real logging framework overrides one method.

### Deltas can say *why*

`Invalid` on its own only ever said "I cannot express this as an `ALTER`", which left the reader to
diff the table by hand to find which change was the stuck one. The new
`ISchemaObjectDeltaWithReason` is implemented on the `TableDelta` of all five providers, and the
reason now appears in three places that previously named only the object: the warning above, the
`SchemaMigrationException` that every other `AutoCreate` raises, and `resources check`.

`resources check` changes its headline as a result. It used to say "Cannot apply a detected database
configuration change!", which is only half true — under `AutoCreate.All` the change *is* applied, by
dropping the object. It now names the object and the reason, and says what `All` would do with it.

### Refusing the branch outright

```csharp
database.Migrator.RefuseDestructiveChanges = true;
```

Off by default. It turns that branch into a `SchemaMigrationException` even under `All`, for teams
who want `All`'s convenience for the additive changes they make all day without the one change that
empties a table. It sits in `WriteUpdate` rather than on the apply path, so it covers `db-patch`
too — a team that turns it on never gets a migration script with the `DROP` in it either. A delta
that can rebuild in place loses nothing and is still applied.

## sp_getapplock failures are decoded, and SQL Server honours ResourceMigrationFailureMode

[#599](https://github.com/JasperFx/weasel/issues/599).

The blocking SQL Server lock helpers threw
`new Exception($"sp_getapplock failed with errorCode '{returnValue}'")` — a bare `System.Exception`
carrying a number nothing decodes, for return codes that are documented and specific. It was also
inconsistent with the PostgreSQL side, where lock contention surfaces as an
`InvalidOperationException` and is ruled on by `ResourceMigrationFailureMode`.

`GlobalLockUnavailableException` now carries the resource, the return code, and what the code means:
`-1` contention with the timeout named, `-2` cancelled, `-3` chosen as a deadlock victim, `-999` a
malformed request. It derives from `InvalidOperationException`, which is what the PostgreSQL path has
always thrown for the same situation, so an existing `catch` keeps working and gains a type it can
narrow to.

`SharedLockExtensions.DefaultLockTimeoutMilliseconds` replaces the hard-coded 1000ms — this was the
only migration-lock timeout in the stack that could not be changed — and every entry point takes an
optional per-call override.

And SQL Server finally has an `IGlobalLock<SqlConnection>`. It had none at all, so
`ApplyAllConfiguredChangesToDatabaseAsync` ran under the nullo lock and the only SQL Server migration
lock in the stack was the one a caller took for itself, where losing the race threw. With
`SqlServerGlobalLock`, contention is an `AttainLockResult` failure that
`ResourceMigrationFailureMode` rules on, so `ContinueOnFailures` means the same thing on both
providers. `-999` still throws: a malformed request is not a busy lock, and treating it as one would
make a coding error look like a rolling deploy.

## Assert reports drift as drift

[#601](https://github.com/JasperFx/weasel/issues/601).

`AssertDatabaseMatchesConfigurationAsync` wrote its DDL with `AutoCreate.CreateOrUpdate`, and
`WriteAllUpdates` runs `AssertPatchingIsValid` before it writes anything. So when a delta was
`Invalid` and not rebuildable in place, the assert aborted with a `SchemaMigrationException` instead
of the `DatabaseValidationException` it documents: `db-assert` printed the generic
`Failed to assert database '{id}'!` headline rather than `does not match the configuration!`, and a
caller doing `catch (DatabaseValidationException)` missed the one case where the drift is worst.

The assert is a report, not an apply, so drift that cannot be applied incrementally is still drift.
The refusal is caught and wrapped, kept as the `InnerException`, and the body shows what the change
would take under `AutoCreate.All` semantics — drop-and-recreate text is fine to show when nothing on
that path is going to run it. Ordinary drift is untouched.

## A numeric-revision concurrency failure always explains itself

[#595](https://github.com/JasperFx/weasel/issues/595).

The numeric closed-shape operations raised their `ConcurrencyException` through the constructor that
only appends the "you may need to call `UpdateRevision()`" hint when the document implements
`IRevisioned` or `ILongVersioned`. A document that opts into numeric revisions any other way — a
`[Version]`-style attribute, or the fluent `UseNumericRevisions` / `Revisioned` configuration — got
the bare message and no hint. Whether a user saw the one sentence that explains the failure depended
on which of several equivalent configuration styles they happened to use.

These operations *are* the numeric operations, so the hint always applies. Because they are the
single throw site for the revision guard across Marten, Polecat and Fisher, this fixes all three at
once.

## A per-event append can translate its own database error

[#596](https://github.com/JasperFx/weasel/issues/596).

`InsertStreamOperationBase` has always carried a per-operation exception transform, so a dialect can
translate a streams-table collision with the `StreamAction` in hand. The per-event append had no
equivalent, so the unique violation on the events table's `(stream_id, version)` key — how a lost
optimistic-concurrency race surfaces on the rich append path — fell through to the store's *global*
transform chain, which sees only the driver exception and has to reconstruct the stream id and
aggregate type by regex over provider-specific error detail the driver may well have redacted.

`TransformAppendEventException` mirrors the insert hook. Additive: a store that does not install a
closure is unchanged.

## Six message defects

[#602](https://github.com/JasperFx/weasel/issues/602). All in text a user reads.

- "Unable to attain a global lock **in time order** to apply database changes" — a typo for "in
  time", and it named no remedy for a situation that on a rolling deploy is the expected outcome for
  every replica but one. It now points at `ResourceMigrationFailureMode.ContinueOnFailures`.
- `$"Unable to drop schema: ${schemaName}"` printed a literal `$` before the name.
- `PostgresqlIdentifierTooLongException` rendered `PostgresqlMigratorNameDataLength` — one token
  short of a property a reader can go and find.
- `PostgresqlIdentifierInvalidException` still claimed "Weasel does not quote identifiers", untrue
  since the 9.x quoting work, so it sent the reader off to rename an object Weasel would have
  delimited perfectly well.
- "Weasel.SqlServer does not (yet) support database type mapping to …" is thrown from the
  provider-neutral builder in `Weasel.Core`, so it named the wrong package on every provider but one.
- The in-process migration lock threw a bare `TimeoutException` — "The operation has timed out." and
  nothing about what had timed out, what was holding it, or what to do instead.

## The AutoCreate documentation was wrong about two modes

[#597](https://github.com/JasperFx/weasel/issues/597). Documentation only; no behaviour changed.

`AutoCreate.None` was documented as "Throws if the database does not match." Neither path that reads
it throws. The lazy per-feature path returns immediately without touching the database, so a missing
table surfaces later as the provider's own error; and the full apply path —
`ApplyAllConfiguredChangesToDatabaseAsync`, and therefore `db-apply` and `resources setup` —
**coerces `None` to `CreateOrUpdate` and migrates anyway**, because an explicit apply is intent to
provision. The only path that reports drift is `db-assert`, which throws regardless of mode. So
`AutoCreate.None` *plus* `db-assert` is the combination that actually fails fast; `None` on its own
only means "do not migrate lazily".

`AutoCreate.CreateOrUpdate` was documented as "Never drops existing objects." It never drops a
*table* the model does not know about, but for a table it does know, the `Update` delta drops columns
present in the database and absent from the model, along with extra indexes and extra foreign keys.
Removing a field from a mapped document under `CreateOrUpdate` drops that column and the data in it.
If you need a strictly additive policy, that is `CreateOnly`.

The behaviour stays as it is; the documentation now describes it.

## JasperFx 2.74.0

`JasperFx` and `JasperFx.Events` move from 2.46.0 to 2.74.0. No source changes were needed.
