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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L12-L18' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_a_table' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L23-L31' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_columns' title='Start of snippet'>anchor</a></sup>
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
- `GeneratedAs(expression)` -- makes this a stored generated column

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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L36-L46' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_primary_keys' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L67-L73' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_foreign_keys' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L78-L89' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_indexes' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Indexes support GIN, GiST, BRIN, and hash methods via the `IndexMethod` enum. Expression-based indexes and sort order (`SortOrder`, `NullsSortOrder`) are also available.

### Full Text Indexes

`FullTextIndexDefinition` builds a GIN index over a `tsvector`. In the ordinary case you give it the
text and it does the conversion:

<!-- snippet: sample_pg_full_text_index -->
<a id='snippet-sample_pg_full_text_index'></a>
```cs
var table = new Table("articles");

// Weasel converts the text for you: to_tsvector('english', data)
table.ModifyColumn("data").AddFullTextIndex();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L94-L99' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_full_text_index' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

#### Weighting with setweight

PostgreSQL's [ranking support](https://www.postgresql.org/docs/current/textsearch-features.html#TEXTSEARCH-MANIPULATE-TSVECTOR)
lets you weight one member above another, so a match in a title outranks the same match in a body.
It works by labelling each member's *vector* and concatenating the vectors — not by concatenating
the text and converting once. The expression is therefore already a `tsvector` at the top level, and
wrapping it in another `to_tsvector` is a type error rather than a weighted index.

Use `FullTextIndexDefinition.ForTsVector` for these, or set `TsVectorExpression` on an existing
definition:

<!-- snippet: sample_pg_weighted_full_text_index -->
<a id='snippet-sample_pg_weighted_full_text_index'></a>
```cs
var table = new Table("articles");

// setweight() labels a tsvector, so weighting concatenates the vectors -- not the text.
// The expression is therefore already a tsvector, and must not be wrapped in another
// to_tsvector call.
var weighted =
    "setweight(to_tsvector('english', coalesce(data ->> 'Title', '')), 'A') || " +
    "setweight(to_tsvector('english', coalesce(data ->> 'Body', '')), 'B')";

var index = FullTextIndexDefinition.ForTsVector(
    PostgresqlObjectName.From(table.Identifier), weighted);

table.Indexes.Add(index);

// Read the indexed vector back off the definition when you build the query-side filter,
// so the vector you search cannot drift from the vector you indexed. A ts_rank computed
// over a different vector than the one @@ filtered on is silently wrong, not just slow.
var where = $"{index.IndexedTsVector} @@ plainto_tsquery('english', :term)";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L104-L123' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_weighted_full_text_index' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Read `IndexedTsVector` — never `DocumentConfig` or `TsVectorExpression` directly — when you build the
query-side filter. It is the one property both the DDL and your query read, whichever way the index
was configured, so the vector you search cannot drift from the vector you indexed.

`DocumentConfig` and `RegConfig` take no part in the DDL once `TsVectorExpression` is set: a
pre-built vector already carries its own text search configuration inside the expression. Leave
`TsVectorExpression` unset and the definition behaves exactly as it always has.

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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L130-L138' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_default_values' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generated Columns

PostgreSQL 12+ supports stored generated columns (`GENERATED ALWAYS AS (...) STORED`). The generation expression is read back from the database catalog by `FetchExisting`, and participates in delta detection with canonicalized expression comparison — changing the expression migrates the column with a lossless drop and re-add (the data is derived). Generated columns the model does not declare are left untouched.

<!-- snippet: sample_pg_generated_columns -->
<a id='snippet-sample_pg_generated_columns'></a>
```cs
var table = new Table("people");

table.AddColumn<string>("first_name");
table.AddColumn<string>("last_name");

// GENERATED ALWAYS AS (...) STORED — PostgreSQL only supports
// stored generated columns. The expression is read back from the
// database catalog and participates in delta detection.
table.AddColumn("full_name", "text")
    .GeneratedAs("first_name || ' ' || last_name");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L51-L62' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_generated_columns' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L143-L159' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_table_delta_detection' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L164-L171' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_table_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
