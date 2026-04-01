# Functions

The `Function` class in `Weasel.SqlServer.Functions` manages T-SQL user-defined functions (scalar and table-valued) with delta detection.

## Creating a Function from SQL

The simplest approach is to use `Function.ForSql()`, which parses the function name from the SQL body:

<!-- snippet: sample_ss_function_from_sql -->
<a id='snippet-sample_ss_function_from_sql'></a>
```cs
var fn = Function.ForSql(@"
CREATE FUNCTION dbo.CalculateDiscount(@Price DECIMAL(10,2), @Rate DECIMAL(5,2))
RETURNS DECIMAL(10,2)
AS
BEGIN
RETURN @Price * @Rate;
END;
");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L201-L210' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_function_from_sql' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Constructor-Based Creation

You can also construct a `Function` directly with an identifier and body:

<!-- snippet: sample_ss_function_constructor -->
<a id='snippet-sample_ss_function_constructor'></a>
```cs
var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.GetUserCount");

var fn = new Function(identifier, @"
CREATE FUNCTION dbo.GetUserCount()
RETURNS INT
AS
BEGIN
RETURN (SELECT COUNT(*) FROM dbo.users);
END;
");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L215-L226' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_function_constructor' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Custom Drop Statements

When a function has overloads or requires special cleanup, provide custom drop statements:

<!-- snippet: sample_ss_function_custom_drop -->
<a id='snippet-sample_ss_function_custom_drop'></a>
```cs
var fn = new Function(identifier, body, new[]
{
    "DROP FUNCTION IF EXISTS dbo.GetUserCount;"
});
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L234-L239' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_function_custom_drop' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Function deltas are detected by querying `sys.sql_modules` and comparing definitions:

<!-- snippet: sample_ss_function_delta_detection -->
<a id='snippet-sample_ss_function_delta_detection'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

var delta = await fn.FindDeltaAsync(conn);
// delta.Difference: None, Create, or Update
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L248-L254' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_function_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Marking for Removal

To generate a drop statement for a function that should be removed:

<!-- snippet: sample_ss_function_for_removal -->
<a id='snippet-sample_ss_function_for_removal'></a>
```cs
var removed = Function.ForRemoval("dbo.ObsoleteFunction");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L259-L261' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_function_for_removal' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
