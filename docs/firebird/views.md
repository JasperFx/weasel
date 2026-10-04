# Views

The `View` class in `Weasel.Firebird.Views` manages a Firebird view: the `SELECT` after `AS`, created, compared and
dropped like any other schema object.

## Defining a View

<!-- snippet: sample_firebird_define_view -->
<a id='snippet-sample_firebird_define_view'></a>
```cs
var view = new View("active_users",
    "SELECT id, name, email FROM users WHERE is_active");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdViewSamples.cs#L16-L19' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_define_view' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A name without a schema lives in `PUBLIC`; any other schema is refused. See
[No schemas before Firebird 6](/firebird/#no-schemas-before-firebird-6).

## Generating DDL

<!-- snippet: sample_firebird_view_ddl -->
<a id='snippet-sample_firebird_view_ddl'></a>
```cs
var migrator = new FirebirdMigrator();
var writer = new StringWriter();

view.WriteCreateStatement(migrator, writer);
// CREATE OR ALTER VIEW active_users AS SELECT id, name, email FROM users WHERE is_active;

view.WriteDropStatement(migrator, writer);
// An EXECUTE BLOCK that drops the view only while it exists
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdViewSamples.cs#L27-L36' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_view_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

| Statement | Written as |
|---|---|
| Create | `CREATE OR ALTER VIEW name AS query`, one plain statement |
| Update | The same statement: the view is altered in place, never dropped first |
| Drop | An `EXECUTE BLOCK` that runs `DROP VIEW` only while the view exists |
| Racing appliers | A `CREATE OR ALTER` that loses a catalog race runs again, so every applier succeeds |

`CREATE OR ALTER VIEW` adds, removes, renames and retypes the view's columns. The views, procedures and triggers that
use it, and the privileges granted on it, survive the change. Firebird refuses one change: removing a column that
something else still uses. Dropping the view first would be refused for the same reason.

A trailing `;` in the query is left off.

## Delta Detection

<!-- snippet: sample_firebird_view_delta -->
<a id='snippet-sample_firebird_view_delta'></a>
```cs
await using var conn = new FbConnection(connectionString);
await conn.OpenAsync();

var delta = await view.FindDeltaAsync(conn);
// Create, Update or None

await view.ApplyChangesAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdViewSamples.cs#L44-L52' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_view_delta' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Firebird keeps the query as it was written, in `RDB$RELATIONS.RDB$VIEW_SOURCE`, so the two texts are compared:

| Difference | Change? |
|---|---|
| Whitespace, line breaks | No |
| Case outside literals | No |
| A trailing `;` | No |
| A literal (`'one'` vs `'ONE'`) | Yes |
| A comment | Yes: it is part of the stored text |

The source is read whole, however long the query. A view named like an existing table reports `Create`, and Firebird
refuses the `CREATE OR ALTER VIEW` rather than replacing the table.

## Teardown

`DropSchemaAsync` drops every view, a view over a view included.
