# Views

Weasel supports both standard views and materialized views via the `Weasel.Postgresql.Views` namespace.

## Standard Views

<!-- snippet: sample_pg_create_standard_view -->
<a id='snippet-sample_pg_create_standard_view'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE is_active = true");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L12-L15' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_standard_view' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Views can be placed in a specific schema:

<!-- snippet: sample_pg_view_in_schema -->
<a id='snippet-sample_pg_view_in_schema'></a>
```cs
var view = new View(
    new DbObjectName("reporting", "monthly_totals"),
    "SELECT date_trunc('month', created_at) AS month, SUM(amount) FROM orders GROUP BY 1");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L20-L24' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_view_in_schema' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Move a view to a different schema after creation:

<!-- snippet: sample_pg_move_view_to_schema -->
<a id='snippet-sample_pg_move_view_to_schema'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE is_active = true");
view.MoveToSchema("analytics");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L29-L33' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_move_view_to_schema' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Materialized Views

Materialized views store query results physically and support custom access methods.

<!-- snippet: sample_pg_create_materialized_view -->
<a id='snippet-sample_pg_create_materialized_view'></a>
```cs
var matView = new MaterializedView("product_stats",
    "SELECT product_id, COUNT(*) as order_count, SUM(amount) as total FROM orders GROUP BY product_id");

// Optionally specify a custom access method (e.g., columnar)
matView.UseAccessMethod("columnar");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L38-L44' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_materialized_view' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Weasel fetches the existing view definition from `pg_catalog` and compares it against the expected SQL. Since PostgreSQL does not support `ALTER VIEW` for changing the query, changes result in a drop-and-recreate.

<!-- snippet: sample_pg_view_exists_check -->
<a id='snippet-sample_pg_view_exists_check'></a>
```cs
var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE is_active = true");

await using var conn = dataSource.CreateConnection();
await conn.OpenAsync();

// Check existence
bool exists = await view.ExistsInDatabaseAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L49-L59' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_view_exists_check' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

When used within a `PostgresqlDatabase`, delta detection runs automatically during `ApplyAllConfiguredChangesToDatabaseAsync()`.

## Generating DDL

The `WriteCreateStatement` method emits both a DROP and CREATE statement to ensure idempotent application.

<!-- snippet: sample_pg_view_generate_ddl -->
<a id='snippet-sample_pg_view_generate_ddl'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE is_active = true");

var migrator = new PostgresqlMigrator();
var writer = new StringWriter();

view.WriteCreateStatement(migrator, writer);
// DROP VIEW IF EXISTS public.active_users;
// CREATE VIEW public.active_users AS SELECT id, name, email FROM users WHERE is_active = true;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L64-L74' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_view_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

You can also generate the SQL inline for diagnostics:

<!-- snippet: sample_pg_view_to_basic_sql -->
<a id='snippet-sample_pg_view_to_basic_sql'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE is_active = true");

string sql = view.ToBasicCreateViewSql();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlViewSamples.cs#L79-L84' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_view_to_basic_sql' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
