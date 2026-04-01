# Setup & Integration

Weasel provides database CLI commands through the [JasperFx](https://jasperfx.github.io/) command line framework. These commands let you apply migrations, validate schema state, generate patch files, and dump DDL from the command line.

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

## Database Discovery

Weasel discovers databases through dependency injection. Register your databases as `IDatabase` or `IDatabaseSource` implementations in the DI container, and the CLI commands will find them automatically.

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
