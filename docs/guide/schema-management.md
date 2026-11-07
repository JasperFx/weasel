# Choosing a Schema Management Approach

Every Critter Stack tool that touches a relational database -- [Marten](https://martendb.io), [Polecat](https://polecat.jasperfx.net), [Fisher](https://fisher.jasperfx.net), [Wolverine](https://wolverinefx.net)'s durable messaging and EF Core integration, and the [CritterWatch](https://critterwatch.jasperfx.net) console -- describes the tables, indexes, functions and sequences it needs as Weasel schema objects, and lets Weasel work out the difference between that description and a live database. What differs between a developer's laptop and a locked-down production database is not the migration engine but *when* it runs and *who* is allowed to run it. This page lays out the four answers to that question, which one to pick for which environment, and where each one is configured.

## The four approaches

| | Who runs the migration | When | Typical use |
|---|---|---|---|
| **1. Lazily, at runtime** | The application, the first time it needs a table | On first use of each feature | Local development |
| **2. At application startup** | The application, in a hosted service | Once, before anything else starts | Development, integration tests, and many production deployments that let the application own its schema |
| **3. As a deployment step** | `dotnet run -- db-apply` or `dotnet run -- resources setup` | Once per deployment, before the new version starts | Production, where the application runs with `AutoCreate.None` |
| **4. From reviewed SQL scripts** | A DBA or a migration tool, from the output of `db-patch` / `db-dump` | Whenever the change control process says so | Production, where the application's credentials cannot run DDL |

They compose. A common recipe is 1 or 2 locally, 3 or 4 in production, and `db-assert` as a gate in both: it tells you whether the database matches the configuration without changing anything.

### 1. Lazily, at runtime

This is the default experience, and the thing that makes a fresh clone of a Critter Stack codebase runnable against an empty database. Each library checks the objects a feature needs the first time that feature is used -- Marten checks a document type's table on the first `Store()` or query against it, Wolverine checks its envelope tables when the durability agent starts -- and creates or alters whatever is missing.

What it is allowed to do is governed by the [`AutoCreate` policy](/core/schema-migrations#autocreate-policy):

| `AutoCreate` | Lazy behaviour |
|---|---|
| `All` | Create, alter, and when an object cannot be altered in place, drop and recreate it. The only mode that can lose a table's data |
| `CreateOrUpdate` (default) | Create what is missing and alter what exists to match, which includes dropping columns and indexes the configuration no longer declares |
| `CreateOnly` | Create what is missing, never alter what exists |
| `None` | Do nothing. A missing table surfaces as the database's own error |

::: warning `None` is not a validation mode
With `AutoCreate.None`, the lazy check simply returns, so a table that is missing or stale shows up later as the provider's error (`42P01 relation does not exist` on PostgreSQL, `Invalid object name` on SQL Server, `no such table` on SQLite) rather than as a message about configuration drift. Validation is a separate, explicit step: `db-assert` or `AssertDatabaseMatchesConfigurationAsync()`. See [the warning on the AutoCreate policy](/core/schema-migrations#autocreate-policy).
:::

### 2. At application startup

Rather than paying for a schema check the first time each feature is used, apply every outstanding change once, before the rest of the application starts. There are two ways to ask for it, and they do the same work:

- **Per store.** Marten, Polecat and Fisher each expose `.ApplyAllDatabaseChangesOnStartup()` on the result of `AddMarten()` / `AddPolecat()` / `AddFisher()`. Wolverine's envelope storage is applied at startup by default (`WolverineOptions.AutoBuildMessageStorageOnStartup`).
- **For everything at once.** `services.AddResourceSetupOnStartup()` from JasperFx runs `Setup()` on every registered [stateful resource](https://shared-libs.jasperfx.net/cli/resources.html#setting-up-resources-at-application-startup): every Weasel database the application knows about *and* the message broker queues, exchanges, topics and subscriptions a Wolverine transport declares. `UseResourceSetupOnStartupInDevelopment()` on the host builder does the same only when the environment is `Development`.

The startup hosted service respects the `AutoCreate` policy. Under `None` it does nothing, so a production deployment can leave the registration in place and still be sure the application never issues DDL.

What happens when the startup migration fails is governed by [`ResourceMigrationFailureMode`](https://shared-libs.jasperfx.net/configuration/jasperfx-options.html#resourcemigrationfailuremode) on the active JasperFx profile: fail the startup (the default) or log and continue, which is what a replica losing the migration lock in a rolling deploy wants.

### 3. As a deployment step

Keep the application itself at `AutoCreate.None`, and run the migration as its own step in the deployment pipeline, with the application's own binary:

```bash
dotnet run -- db-apply            # Weasel databases only
dotnet run -- resources setup     # every stateful resource, broker objects included
```

Both are an *explicit* instruction to migrate, so both apply the migration as `CreateOrUpdate` even when the configured policy is `None`. `CreateOrUpdate` never drops a table, so this is safe to run against a database with data in it. Follow it with `db-assert` or `resources check` and the pipeline fails before traffic is cut over if anything did not land.

This is what the Critter Stack recommends for production when the deployment identity is allowed to run DDL. It needs no migration history table and no hand-written scripts: the configured schema *is* the migration. The [JasperFx.Aspire](https://shared-libs.jasperfx.net/cli/aspire.html) package exposes the same `resources setup` as a startup gate and a dashboard button for Aspire-hosted applications.

See [Command Line Tools](/cli/) for enabling the commands, and [db-apply](/cli/db-apply) for the `--parallel` flag and the per-database reporting that matter when a tenant-per-database store has hundreds of databases.

### 4. From reviewed SQL scripts

When the application's credentials cannot run DDL at all, generate the SQL and hand it to whoever can:

```bash
dotnet run -- db-dump schema.sql          # the whole schema, for a new environment
dotnet run -- db-patch migrations/42.sql  # the delta against the live database, plus a rollback file
```

[db-patch](/cli/db-patch) connects to the database, compares it to the configuration, and writes the forward migration and a `.drop` rollback. It changes nothing. The output can be reviewed in a pull request, stored with the application's source, and applied by a database migration tool such as [grate](https://grate-devs.github.io/grate/) as the [Marten DevOps guide](https://martendb.io/devops/devops.html) shows, or by a DBA directly. Run the application with `AutoCreate.None` so it never attempts the DDL itself, and use `db-assert` in the pipeline to prove the scripts were applied.

Teams that have standardised on EF Core migrations have a fifth option: [EF Core migration generation](/efcore/migration-generation) emits the same Weasel schema model as versioned EF Core migration files that `dotnet ef database update` applies.

## Where the policy is set

| Library | Setting | Default |
|---|---|---|
| Marten | `StoreOptions.AutoCreateSchemaObjects` | The active JasperFx profile's `ResourceAutoCreate`, itself `CreateOrUpdate` |
| Polecat | `StoreOptions.AutoCreateSchemaObjects` | `CreateOrUpdate` |
| Fisher | `StoreOptions.AutoCreateSchemaObjects` | `CreateOrUpdate` |
| Wolverine envelope storage and database transports | `WolverineOptions.AutoBuildMessageStorageOnStartup` | The active JasperFx profile's `ResourceAutoCreate`, itself `CreateOrUpdate` |
| Wolverine EF Core `DbContext` types | `opts.UseEntityFrameworkCoreWolverineManagedMigrations()` opts in; the migration then follows the storage setting above | Off |
| Weasel used directly | `IDatabase.AutoCreate` | Whatever your `DatabaseBase` subclass sets |

Marten and Wolverine both read their default from the JasperFx profile, so the one-liner that turns off automatic migration for both in production is:

```cs
builder.Services.CritterStackDefaults(x =>
{
    x.Production.ResourceAutoCreate = AutoCreate.None;
});
```

See [Critter Stack Defaults](https://shared-libs.jasperfx.net/configuration/critter-stack-defaults.html) for the profile model.

## What each library documents

Each library's own documentation covers how these approaches apply to its tables, which is where the store-specific caveats live:

- Marten: [Schema Migrations and Patches](https://martendb.io/schema/migrations.html) and [Command Line Tooling](https://martendb.io/configuration/cli.html)
- Polecat: [Schema Migrations](https://polecat.jasperfx.net/schema/migrations.html)
- Fisher: [Database Management](https://fisher.jasperfx.net/schema/) and [Schema Migrations](https://fisher.jasperfx.net/schema/migrations.html), including what SQLite cannot `ALTER`
- Wolverine: [Managing Message Storage](https://wolverinefx.net/guide/durability/managing.html) and [EF Core Database Migrations](https://wolverinefx.net/guide/durability/efcore/migrations.html)
- CritterWatch: [Database Schema and Permissions](https://critterwatch.jasperfx.net/deployment/database.html), a worked example of running a Weasel-managed schema under a DBA-owned role
- JasperFx: [Stateful Resources](https://shared-libs.jasperfx.net/cli/resources.html), the `resources` command and startup setup

## Checking rather than changing

Whichever approach applies the schema, two commands only ever read:

```bash
dotnet run -- db-assert          # non-zero exit if any database differs from the configuration
dotnet run -- resources check    # the same, across every stateful resource
```

Both work regardless of the `AutoCreate` setting, so `AutoCreate.None` plus `db-assert` in the pipeline is the combination that actually fails fast on drift. `db-assert` also reports when *every* object came back missing, which points at a role that cannot see the catalog rather than at a missing schema -- see [Introspection is privilege-filtered](/core/schema-migrations#introspection-is-privilege-filtered).
