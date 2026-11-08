# Setup & Integration

Weasel provides database CLI commands through the [JasperFx](https://shared-libs.jasperfx.net/cli/) command line framework. These commands let you apply migrations, validate schema state, generate patch files, and dump DDL from the command line.

If you are here from Marten, Polecat, Fisher, Wolverine or CritterWatch: these are the `db-*` commands those tools list under `dotnet run -- help`. Each of them registers its databases with Weasel, so the commands below work the same way against a document store, Wolverine's envelope storage, a database-backed Wolverine transport, or an EF Core `DbContext` that Wolverine manages. See [Choosing a Schema Management Approach](/guide/schema-management) for where these commands fit against runtime migration and startup setup.

::: tip
The command line framework itself is documented with JasperFx: [setup](https://shared-libs.jasperfx.net/cli/),
[writing your own commands](https://shared-libs.jasperfx.net/cli/writing-commands),
[arguments and flags](https://shared-libs.jasperfx.net/cli/arguments-flags),
[environment checks](https://shared-libs.jasperfx.net/cli/environment-checks) and
[`describe`](https://shared-libs.jasperfx.net/cli/describe). This section covers the `db-*` commands Weasel adds.
:::

::: info Native AOT
Since JasperFx 2.101.0 the command line runs from a Native AOT binary, as long as every assembly that
contributes commands is built with `JasperFx.SourceGenerator`. Weasel.Core and Weasel.EntityFrameworkCore
are, from Weasel 9.50.0 on, so `db-apply`, `db-assert`, `db-patch`, `db-dump`, `db-list` and `db-ef-migration`
all appear in `help` and run from the native binary. See [Native AOT](https://shared-libs.jasperfx.net/cli/#native-aot)
in the JasperFx documentation for what your application needs.
:::

## Enabling the CLI

To enable the Weasel CLI commands, make two changes to your `Program.cs`:

<!-- snippet: sample_cli_enable_weasel_cli -->
<a id='snippet-sample_cli_enable_weasel_cli'></a>
```cs
var builder = WebApplication.CreateBuilder(args);

// 1. Add this call to enable JasperFx extensions
builder.Host.ApplyJasperFxExtensions();

// ... configure services as usual ...

var app = builder.Build();

// 2. Replace app.Run() with this:
return await app.RunJasperFxCommands(args);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CliSamples.cs#L13-L25' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_cli_enable_weasel_cli' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`RunJasperFxCommands(args)` will intercept any recognized CLI commands and run them instead of starting the web host. If no CLI command is detected, the application starts normally.

## Available Commands

Run `dotnet run -- help` to see all available commands:

| Command | Description |
|---------|-------------|
| `db-apply` | Applies all outstanding changes to the database(s) |
| `db-assert` | Asserts that the existing database(s) match the current configuration |
| `db-patch` | Exports a SQL patch and rollback file for pending changes |
| `db-dump` | Dumps the entire DDL for the configured database(s) |
| `db-list` | Lists all configured databases |

The `resources` command is not a Weasel command, but every Weasel database is also exposed through it. See [The `resources` command](#the-resources-command) below.

## Common Flags

All database commands support the following flags:

| Flag | Description |
|------|-------------|
| `--database` / `-d` | Filter to a specific database by partial URI match |
| `--conn` / `-c` | Override the connection string |
| `--log` | Record command output to a file |
| `--verbose` | Enable verbose output |
| `--log-level` | Override the log level |
| `--environment` | Override the hosting environment name |

`db-apply` and `db-assert` also take `--parallel <n>` to walk a fleet of databases concurrently. See [db-apply](/cli/db-apply#parallelism).

## Database Discovery

Weasel discovers databases through dependency injection. Register your databases as `IDatabase` or `IDatabaseSource` implementations in the DI container, and the CLI commands will find them automatically.

The Critter Stack libraries do this registration for you:

| Library | What it registers |
|---------|-------------------|
| [Marten](https://martendb.io/schema/migrations.html) | The document store's database, every tenant database under database-per-tenant, and each ancillary store |
| [Polecat](https://polecat.jasperfx.net/schema/migrations.html) | The same, for SQL Server |
| [Fisher](https://fisher.jasperfx.net/schema/) | The same, for SQLite files |
| [Wolverine](https://wolverinefx.net/guide/durability/managing.html) | Its envelope storage, the queue tables of a database-backed transport, and any [EF Core `DbContext`](https://wolverinefx.net/guide/durability/efcore/migrations.html) it manages |
| [CritterWatch](https://critterwatch.jasperfx.net/deployment/database.html) | The console's own store and durability schema |

`db-list` shows what was found. When an application has more than one, pass the `SubjectUri` it prints to `--database` / `-d` to target one of them, for example `-d marten://store/` or `-d wolverine://messages/main`.

## The `resources` command

Weasel's `db-*` commands act on Weasel databases only. The JasperFx [stateful resource](https://shared-libs.jasperfx.net/cli/resources.html) model is wider: a database schema, a Rabbit MQ exchange, an Azure Service Bus topic, or an SQS queue are all an `IStatefulResource`, and the `resources` command acts on every one the application has registered.

```bash
dotnet run -- resources list      # every stateful resource in the application
dotnet run -- resources check     # verify each one, non-zero exit on any failure
dotnet run -- resources setup     # create or migrate each one
dotnet run -- resources clear     # wipe persisted state, keep the structure
dotnet run -- resources teardown  # remove each one
```

Every Weasel database registered through an `IDatabaseSource` is one of those resources, so `resources setup` applies the same migration `db-apply` does, and `resources check` runs the same comparison `db-assert` does. Use the `db-*` commands when you want the database-specific options (`db-patch`, `db-dump`, `--parallel`); use `resources` when one deployment step should provision the databases and the message broker objects together.

::: tip
`resources setup` and `db-apply` are both an *explicit* instruction to migrate, so both apply the migration as `CreateOrUpdate` even when the database's `AutoCreate` is `None`. `resources check` and `db-assert` never change anything. See [AutoCreate Policy](/core/schema-migrations#autocreate-policy).
:::

The same `Setup()` can run at application startup instead of from the command line, with `services.AddResourceSetupOnStartup()`. The [JasperFx documentation](https://shared-libs.jasperfx.net/cli/resources.html#setting-up-resources-at-application-startup) covers that, the `IHost.SetupResources()` / `ResetResourceState()` helpers for tests, and how to write a resource of your own.

## Integration Testing

If you need the host to start automatically during integration testing (without CLI arguments), set:

<!-- snippet: sample_cli_auto_start_host -->
<a id='snippet-sample_cli_auto_start_host'></a>
```cs
JasperFxEnvironment.AutoStartHost = true;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CliSamples.cs#L30-L32' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_cli_auto_start_host' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This bypasses the CLI argument check and starts the host directly.
