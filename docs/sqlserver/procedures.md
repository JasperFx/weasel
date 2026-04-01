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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L120-L134' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_define_stored_procedure' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_ss_procedure_ddl -->
<a id='snippet-sample_ss_procedure_ddl'></a>
```cs
var migrator = new SqlServerMigrator();
var writer = new StringWriter();

// CREATE PROCEDURE
proc.WriteCreateStatement(migrator, writer);

// CREATE OR ALTER PROCEDURE (for updates)
proc.WriteCreateOrAlterStatement(migrator, writer);

// DROP PROCEDURE IF EXISTS
proc.WriteDropStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L142-L154' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_procedure_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L163-L176' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_procedure_delta_detection' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L188-L194' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_procedure_fetch_existing' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
