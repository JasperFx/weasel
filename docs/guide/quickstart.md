# Quick Start

This guide shows the simplest way to use Weasel: define a table in code and migrate it to your database with a single method call.

## PostgreSQL

<!-- snippet: sample_postgresql_quickstart -->
<a id='snippet-sample_postgresql_quickstart'></a>
```cs
// Define a table
var table = new Weasel.Postgresql.Tables.Table("myapp.people");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("first_name").NotNull();
table.AddColumn<string>("last_name").NotNull();
table.AddColumn<string>("email");

// Add a unique index on email
table.ModifyColumn("email").AddIndex(i => i.IsUnique = true);

// Connect and migrate
var dataSource = NpgsqlDataSource.Create("Host=localhost;Database=mydb");
await using var conn = await dataSource.OpenConnectionAsync();
await table.MigrateAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/QuickStartSamples.cs#L14-L29' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_postgresql_quickstart' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## SQL Server

<!-- snippet: sample_sqlserver_quickstart -->
<a id='snippet-sample_sqlserver_quickstart'></a>
```cs
// Define a table
var table = new Weasel.SqlServer.Tables.Table(new DbObjectName("dbo", "people"));
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("first_name").NotNull();
table.AddColumn<string>("last_name").NotNull();

// Connect and migrate
await using var conn = new SqlConnection("Server=localhost;Database=mydb;Trusted_Connection=True;TrustServerCertificate=True");
await conn.OpenAsync();
await table.MigrateAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/QuickStartSamples.cs#L34-L45' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlserver_quickstart' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## What Does MigrateAsync Do?

`MigrateAsync()` is a convenience method that handles the full migration lifecycle in one call:

1. **If the table does not exist** -- generates and executes the `CREATE TABLE` statement along with any indexes, foreign keys, and constraints.
2. **If the table already exists** -- compares the code-defined table against the actual database schema, detects the differences (added columns, changed types, new indexes, etc.), and applies only the incremental changes needed to bring the database in line with your definition.

This means you can evolve your table definitions over time and simply call `MigrateAsync()` on application startup to keep your database schema in sync.

## Next Steps

See the Core Concepts section for a deeper look at schema objects, delta detection, and the full migration infrastructure.
