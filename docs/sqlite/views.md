# Views

The `View` class in `Weasel.Sqlite.Views` provides CREATE/DROP support with automatic delta detection for SQLite views.

## Creating a View

<!-- snippet: sample_sqlite_create_view -->
<a id='snippet-sample_sqlite_create_view'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE active = 1");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L129-L132' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_create_view' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

Views use a drop-then-create strategy since SQLite has no `ALTER VIEW` support:

<!-- snippet: sample_sqlite_view_ddl -->
<a id='snippet-sample_sqlite_view_ddl'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE active = 1");

var migrator = new SqliteMigrator();
var writer = new StringWriter();
view.WriteCreateStatement(migrator, writer);

// Output:
// DROP VIEW IF EXISTS "active_users";
// CREATE VIEW "active_users" AS SELECT id, name, email FROM users WHERE active = 1;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L137-L148' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_view_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Schema Support

Views support `main` (default) and `temp` schemas:

<!-- snippet: sample_sqlite_view_schema -->
<a id='snippet-sample_sqlite_view_schema'></a>
```cs
// Temporary view (connection-scoped)
var tempView = new View(
    new SqliteObjectName("temp", "session_summary"),
    "SELECT session_id, COUNT(*) as event_count FROM temp.session_data GROUP BY session_id");

// DDL: DROP VIEW IF EXISTS "temp"."session_summary";
// CREATE VIEW "temp"."session_summary" AS SELECT ...
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L153-L161' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_view_schema' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Complex View Examples

Views can use JOINs, aggregations, and JSON extraction:

<!-- snippet: sample_sqlite_complex_views -->
<a id='snippet-sample_sqlite_complex_views'></a>
```cs
// Aggregation with JOIN
var orderSummary = new View("user_order_summary", @"
SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name");

// JSON extraction
var productDetails = new View("product_details", @"
SELECT id, name,
json_extract(metadata, '$.category') as category,
json_extract(metadata, '$.price') as price
FROM products");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L166-L180' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_complex_views' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

The `ViewDelta` class detects changes between expected and actual view definitions using whitespace-insensitive SQL comparison:

<!-- snippet: sample_sqlite_view_delta_detection -->
<a id='snippet-sample_sqlite_view_delta_detection'></a>
```cs
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();

var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE active = 1");

// Check if view exists
var exists = await view.ExistsInDatabaseAsync(connection);

// Fetch current definition from sqlite_master
var existing = await view.FetchExistingAsync(connection);

// Compare expected vs actual
var expectedView = view;
var actualView = existing;
var delta = new ViewDelta(expectedView, actualView);

switch (delta.Difference)
{
    case SchemaPatchDifference.None:
        // View matches expected definition
        break;
    case SchemaPatchDifference.Create:
        // View does not exist yet
        break;
    case SchemaPatchDifference.Update:
        // View SQL changed, will drop and recreate
        break;
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L185-L215' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_view_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Limitations

- SQLite views are read-only (no INSERT, UPDATE, or DELETE)
- No materialized views (use tables with triggers as an alternative)
- Temporary views cannot reference main schema tables
- Changes always require DROP + CREATE (handled automatically by ViewDelta)
