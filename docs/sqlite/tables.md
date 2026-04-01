# Tables

The `Table` class in `Weasel.Sqlite.Tables` provides full DDL generation, delta detection, and schema migration for SQLite tables.

## Creating a Table

<!-- snippet: sample_sqlite_create_table -->
<a id='snippet-sample_sqlite_create_table'></a>
```cs
var table = new Table("users");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email").NotNull();
table.AddColumn("settings", "TEXT"); // raw type
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L37-L43' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_create_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Primary Keys and AUTOINCREMENT

Use `AsPrimaryKey()` and `AutoIncrement()` for `INTEGER PRIMARY KEY AUTOINCREMENT` columns:

<!-- snippet: sample_sqlite_autoincrement -->
<a id='snippet-sample_sqlite_autoincrement'></a>
```cs
var table = new Table("users");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L48-L51' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_autoincrement' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generated Columns

SQLite 3.31+ supports `GENERATED ALWAYS AS` columns with `Stored` or `Virtual` types:

<!-- snippet: sample_sqlite_generated_columns -->
<a id='snippet-sample_sqlite_generated_columns'></a>
```cs
var table = new Table("users");
table.AddColumn("email_domain", "TEXT")
    .GeneratedAs("substr(email, instr(email, '@') + 1)", GeneratedColumnType.Stored);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L56-L60' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_generated_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Foreign Keys

Foreign keys must be defined inline at table creation. SQLite does not support `ALTER TABLE ADD CONSTRAINT`:

<!-- snippet: sample_sqlite_foreign_keys -->
<a id='snippet-sample_sqlite_foreign_keys'></a>
```cs
var orders = new Table("orders");
orders.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
orders.AddColumn<int>("user_id").NotNull();

// Define foreign key referencing users table
orders.ForeignKeys.Add(new ForeignKey("fk_orders_user")
{
    ColumnNames = new[] { "user_id" },
    LinkedTable = new SqliteObjectName("users"),
    LinkedNames = new[] { "id" }
});
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L65-L77' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_foreign_keys' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

::: warning
Foreign key enforcement is disabled by default in SQLite. Enable it via PRAGMA settings using [SqliteHelper](/sqlite/helper).
:::

## Indexes

Add indexes using `IndexDefinition`, including expression and partial indexes:

<!-- snippet: sample_sqlite_indexes -->
<a id='snippet-sample_sqlite_indexes'></a>
```cs
var table = new Table("users");

// Unique index
var emailIdx = new IndexDefinition("idx_email") { IsUnique = true };
emailIdx.AgainstColumns("email");
table.Indexes.Add(emailIdx);

// Expression index on JSON path
var jsonIdx = new IndexDefinition("idx_settings_theme");
jsonIdx.ForJsonPath("settings", "$.theme");
table.Indexes.Add(jsonIdx);

// Partial index with WHERE clause
var activeIdx = new IndexDefinition("idx_active_users");
activeIdx.AgainstColumns("name");
activeIdx.Predicate = "active = 1";
table.Indexes.Add(activeIdx);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L82-L100' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_indexes' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## STRICT Mode and WITHOUT ROWID

Enable strict type checking (SQLite 3.37+) or the WITHOUT ROWID optimization:

<!-- snippet: sample_sqlite_strict_and_without_rowid -->
<a id='snippet-sample_sqlite_strict_and_without_rowid'></a>
```cs
var table = new Table("users");
table.StrictTypes = true;   // CREATE TABLE ... (...) STRICT
table.WithoutRowId = true;  // CREATE TABLE ... (...) WITHOUT ROWID
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L105-L109' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_strict_and_without_rowid' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Schema Support

Tables default to the `main` schema. Use `SqliteObjectName` or `MoveToSchema` for temporary tables:

<!-- snippet: sample_sqlite_schema_support -->
<a id='snippet-sample_sqlite_schema_support'></a>
```cs
// Temporary table
var temp = new Table(new SqliteObjectName("temp", "session_data"));
// DDL: CREATE TABLE IF NOT EXISTS "temp"."session_data" (...)

// Move existing table definition to temp schema
var table = new Table("users");
table.MoveToSchema("temp");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L114-L122' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_schema_support' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection and Table Recreation

Weasel detects differences between expected and actual schema states. When a change cannot be applied with `ALTER TABLE` (such as adding a constraint or changing a column type), Weasel automatically recreates the table:

1. Create new table with the desired schema
2. Copy data with `INSERT INTO new_table SELECT * FROM old_table`
3. Drop the old table
4. Rename the new table

Supported incremental changes include `ADD COLUMN`, `DROP COLUMN` (3.35+), and `RENAME COLUMN` (3.25+).
