# Functions

The `Function` class in `Weasel.Postgresql.Functions` represents a PL/pgSQL (or SQL) function as a schema object with full delta detection support.

## Creating a Function

The simplest way to define a function is with `Function.ForSql()`, which parses the identifier from the SQL body:

<!-- snippet: sample_pg_create_function_from_sql -->
<a id='snippet-sample_pg_create_function_from_sql'></a>
```cs
var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
SELECT amount * rate;
$$;
");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlFunctionSamples.cs#L12-L21' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_function_from_sql' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Or construct directly with an identifier and body:

<!-- snippet: sample_pg_create_function_with_identifier -->
<a id='snippet-sample_pg_create_function_with_identifier'></a>
```cs
var function = new Function(
    new DbObjectName("public", "calculate_tax"),
    @"CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
SELECT amount * rate;
$$;"
);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlFunctionSamples.cs#L26-L36' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_function_with_identifier' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Function Delta Detection

Weasel compares the expected function body against the actual definition stored in `pg_proc` to detect changes.

<!-- snippet: sample_pg_function_delta_detection -->
<a id='snippet-sample_pg_function_delta_detection'></a>
```cs
var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
SELECT amount * rate;
$$;
");

await using var conn = dataSource.CreateConnection();
await conn.OpenAsync();

// Fetch the existing function from the database
var existing = await function.FetchExistingAsync(conn);

// Compute the delta
var delta = await function.FindDeltaAsync(conn);
// delta.Difference: None, Create, or Update
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlFunctionSamples.cs#L41-L61' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_function_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The `FunctionDelta` class handles body comparison and generates the appropriate CREATE or DROP statements.

## Removing a Function

Mark a function for removal during migration:

<!-- snippet: sample_pg_function_for_removal -->
<a id='snippet-sample_pg_function_for_removal'></a>
```cs
var removed = Function.ForRemoval("public.old_function");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlFunctionSamples.cs#L66-L68' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_function_for_removal' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_pg_function_generate_ddl -->
<a id='snippet-sample_pg_function_generate_ddl'></a>
```cs
var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
SELECT amount * rate;
$$;
");

var migrator = new PostgresqlMigrator();
var writer = new StringWriter();

// CREATE FUNCTION statement
function.WriteCreateStatement(migrator, writer);

// DROP FUNCTION statement
function.WriteDropStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlFunctionSamples.cs#L73-L91' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_function_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The drop statement is generated automatically from the function signature, including parameter types for proper overload resolution.

## SQL Templates

Functions support template substitution for schema, function name, and signature placeholders:

<!-- snippet: sample_pg_function_build_template -->
<a id='snippet-sample_pg_function_build_template'></a>
```cs
var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
SELECT amount * rate;
$$;
");

string result = function.BuildTemplate(
    "GRANT EXECUTE ON FUNCTION {SIGNATURE} TO app_user;");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlFunctionSamples.cs#L96-L108' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_function_build_template' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Available placeholders: `{SCHEMA}`, `{FUNCTION}`, `{SIGNATURE}`.
