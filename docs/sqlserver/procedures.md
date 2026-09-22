# Stored Procedures

The `StoredProcedure` class in `Weasel.SqlServer.Procedures` manages T-SQL stored procedures as schema objects with full delta detection support.

## Defining a Stored Procedure

Provide the complete T-SQL body when constructing the stored procedure:

<!-- snippet: sample_ss_define_stored_procedure -->
<a id='snippet-sample_ss_define_stored_procedure'></a>
```cs
var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.usp_get_active_users");

var proc = new StoredProcedure(identifier, @"
CREATE PROCEDURE dbo.usp_get_active_users
@MinAge INT = 18
AS
BEGIN
SET NOCOUNT ON;
SELECT Id, Name, Email
FROM dbo.users
WHERE Active = 1 AND Age >= @MinAge;
END;
");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L139-L153' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_define_stored_procedure' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_ss_procedure_ddl -->
<a id='snippet-sample_ss_procedure_ddl'></a>
```cs
var migrator = new SqlServerMigrator();
var writer = new StringWriter();

// CREATE OR ALTER PROCEDURE, between GO lines
proc.WriteCreateStatement(migrator, writer);

// The same text: one form is safe on both paths
proc.WriteCreateOrAlterStatement(migrator, writer);

// DROP PROCEDURE IF EXISTS
proc.WriteDropStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L161-L173' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_procedure_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`WriteCreateStatement` and `WriteCreateOrAlterStatement` emit the same thing, because only one form
is safe to run twice: `CREATE OR ALTER PROCEDURE`, on a line of its own, between two `GO` lines.
The separators are what make a rendered migration runnable at all, since SQL Server requires
`CREATE OR ALTER PROCEDURE` to be the first statement of its batch and a migration concatenates
every object's DDL into one script. See
[batch separators and re-runnable scripts](/sqlserver/#batch-separators-and-re-runnable-scripts).

You do not have to author the body that way. Whatever the leading keyword is, `CREATE PROCEDURE`,
`CREATE PROC`, `CREATE OR ALTER PROC` or `CREATE OR ALTER PROCEDURE`, it is normalised to
`CREATE OR ALTER PROCEDURE` on the way out. The `GO` lines are written around the body rather than
folded into it, so the text compared against `sys.sql_modules` never sees them, and a body already
authored as `CREATE OR ALTER PROCEDURE` compares equal to what the catalog holds instead of
reporting a permanent `Update`.

## Delta Detection

The `StoredProcedureDelta` compares the expected procedure body against what exists in the database by querying `sys.sql_modules`:

<!-- snippet: sample_ss_procedure_delta_detection -->
<a id='snippet-sample_ss_procedure_delta_detection'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

var delta = await proc.FindDeltaAsync(conn);
if (delta.Difference == SchemaPatchDifference.Create)
{
    // Procedure does not exist yet
}
else if (delta.Difference == SchemaPatchDifference.Update)
{
    // Procedure body has changed
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L182-L195' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_procedure_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Fetching Existing Definitions

<!-- snippet: sample_ss_procedure_fetch_existing -->
<a id='snippet-sample_ss_procedure_fetch_existing'></a>
```cs
var existing = await proc.FetchExistingAsync(conn);
if (existing != null)
{
    // existing contains the current procedure body from the database
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L207-L213' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_procedure_fetch_existing' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
