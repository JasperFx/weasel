# Tables

The `Table` class in `Weasel.Postgresql.Tables` provides a fluent API for defining PostgreSQL tables with columns, primary keys, indexes, foreign keys, and default values.

## Creating a Table

<!-- snippet: sample_pg_create_a_table -->
<a id='snippet-sample_pg_create_a_table'></a>
```cs
// Create a table in the default "public" schema
var table = new Table("users");

// Create a table in a specific schema
var schemaTable = new Table("myschema.users");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L11-L17' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_a_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Adding Columns

Use `AddColumn<T>(name)` to map from .NET types, or `AddColumn(name, type)` to specify the PostgreSQL type directly.

<!-- snippet: sample_pg_add_columns -->
<a id='snippet-sample_pg_add_columns'></a>
```cs
var table = new Table("users");

table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email").NotNull();
table.AddColumn<DateTime>("created_at").NotNull();
table.AddColumn("metadata", "jsonb");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L22-L30' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The fluent `ColumnExpression` returned by `AddColumn` supports:

- `AsPrimaryKey()` -- marks the column as part of the primary key
- `NotNull()` -- disallows NULL values
- `AllowNulls()` -- explicitly allows NULL (the default)
- `DefaultValue(value)` -- sets a default for int, long, or double
- `DefaultValueByString(value)` -- sets a string default (wrapped in quotes)
- `DefaultValueByExpression(expr)` -- sets a raw SQL default expression
- `DefaultValueFromSequence(sequence)` -- uses `nextval()` from a sequence
- `ForeignKeyTo(table, column)` -- adds an inline foreign key

## Primary Keys

Single-column and composite primary keys are supported.

<!-- snippet: sample_pg_primary_keys -->
<a id='snippet-sample_pg_primary_keys'></a>
```cs
var table = new Table("orders");

// Single column
table.AddColumn<Guid>("id").AsPrimaryKey();

// Composite key
var compositeTable = new Table("tenant_orders");
compositeTable.AddColumn<int>("tenant_id").AsPrimaryKey();
compositeTable.AddColumn<int>("order_id").AsPrimaryKey();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L35-L45' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_primary_keys' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

You can customize the primary key constraint name via `table.PrimaryKeyName`.

## Foreign Keys

<!-- snippet: sample_pg_foreign_keys -->
<a id='snippet-sample_pg_foreign_keys'></a>
```cs
var table = new Table("employees");

table.AddColumn<int>("company_id")
    .ForeignKeyTo("companies", "id",
        onDelete: CascadeAction.Cascade);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L50-L56' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_foreign_keys' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Or add foreign keys directly to the `ForeignKeys` collection for multi-column keys.

## Indexes

<!-- snippet: sample_pg_indexes -->
<a id='snippet-sample_pg_indexes'></a>
```cs
var table = new Table("users");

// Simple unique index
var index = new IndexDefinition("idx_users_email")
{
    IsUnique = true,
    Method = IndexMethod.btree
};
index.Columns = new[] { "email" };
table.Indexes.Add(index);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L61-L72' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_indexes' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Indexes support GIN, GiST, BRIN, and hash methods via the `IndexMethod` enum. Expression-based indexes and sort order (`SortOrder`, `NullsSortOrder`) are also available.

## Default Values

<!-- snippet: sample_pg_default_values -->
<a id='snippet-sample_pg_default_values'></a>
```cs
var table = new Table("tasks");

table.AddColumn<bool>("is_active").DefaultValueByExpression("true");
table.AddColumn<int>("priority").DefaultValue(0);
table.AddColumn<string>("status").DefaultValueByString("pending");
table.AddColumn<DateTimeOffset>("created_at")
    .DefaultValueByExpression("now()");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L77-L85' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_default_values' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection and Migration

Weasel compares the expected table definition against the actual database state and generates incremental DDL.

<!-- snippet: sample_pg_table_delta_detection -->
<a id='snippet-sample_pg_table_delta_detection'></a>
```cs
var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
var table = new Table("users");

await using var conn = dataSource.CreateConnection();
await conn.OpenAsync();

// Check if table exists
bool exists = await table.ExistsInDatabaseAsync(conn);

// Fetch the existing table definition from the database
var existing = await table.FetchExistingAsync(conn);

// Compare and generate migration DDL
var delta = new TableDelta(table, existing);
// delta.Difference tells you: None, Create, Update, or Recreate
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L90-L106' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_table_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_pg_table_generate_ddl -->
<a id='snippet-sample_pg_table_generate_ddl'></a>
```cs
var table = new Table("users");

var migrator = new PostgresqlMigrator();
var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);
Console.WriteLine(writer.ToString());
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L111-L118' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_table_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
