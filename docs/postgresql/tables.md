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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L13-L19' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_a_table' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L24-L32' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_columns' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L37-L47' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_primary_keys' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L100-L106' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_foreign_keys' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L111-L122' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_indexes' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L127-L132' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_full_text_index' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L137-L156' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_weighted_full_text_index' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L163-L171' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_default_values' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L52-L63' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_generated_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Storage Parameters

Table level storage parameters -- PostgreSQL's `reloptions`, written as `CREATE TABLE ... WITH (...)` --
let a single table be tuned without touching the server's own defaults. The usual reasons are a lower
`fillfactor` on a table whose rows are updated in place, and tighter autovacuum thresholds on a table
that churns far faster than the rest of the database.

Weasel keeps them in step like any other part of the table: they are written on create, read back from
the catalog, and compared by the delta.

### Setting them

`Table.StorageParameters` is an ordered name/value collection, the same shape as
`IndexDefinition.StorageParameters`. Use the names on `StorageParameterNames` rather than literals:

<!-- snippet: sample_pg_storage_parameters -->
<a id='snippet-sample_pg_storage_parameters'></a>
```cs
var table = new Table("public.events");
table.AddColumn<Guid>("id").AsPrimaryKey();

// The names are case sensitive as dictionary keys but are normalized to lower case when
// written, so two spellings of one parameter would render as one duplicated setting.
// StorageParameterNames is what keeps that from being possible.
table.StorageParameters[StorageParameterNames.FillFactor] = 70;
table.StorageParameters[StorageParameterNames.AutovacuumVacuumScaleFactor] = 0.05;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L68-L77' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_storage_parameters' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The constants are not only convenience. `StorageParameters` keys are case **sensitive**, while the DDL
writer and the catalog reader both normalize to lower case -- so `"FILLFACTOR"` and `"fillfactor"` are two
entries that render as one duplicated setting. Weasel refuses that with a clear message rather than letting
PostgreSQL fail with 22023, but the constants make it unreachable.

There is also a fluent form, which routes through the same constants:

<!-- snippet: sample_pg_storage_parameters_fluent -->
<a id='snippet-sample_pg_storage_parameters_fluent'></a>
```cs
var table = new Table("public.events");
table.AddColumn<Guid>("id").AsPrimaryKey();

table.WithFillFactor(70)
    // Only the arguments supplied are declared, so a hot table can be given just the
    // thresholds that matter to it
    .WithAutovacuum(vacuumScaleFactor: 0.01, insertScaleFactor: 0.02)
    .WithParallelWorkers(4)
    .WithAutovacuumLogging(250.Milliseconds())

    // ...and anything without its own method still goes through the constants
    .WithStorageParameter(StorageParameterNames.VacuumTruncate, false);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L82-L95' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_storage_parameters_fluent' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`WithAutovacuum` declares only the arguments actually supplied. That matters because of how the delta
treats absence -- see below -- so leaving a parameter out is "say nothing about this one", not "reset it".

Either way the table is created with:

```sql
CREATE TABLE IF NOT EXISTS public.events (
    id uuid NOT NULL,
    CONSTRAINT pkey_events_id PRIMARY KEY (id)
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.05);
```

### What the delta does, and does not, touch

Delta detection reads `pg_class.reloptions` and compares **only the parameters the table declares**. A
parameter that exists in the database but is not declared on the table is never reset, because someone
else -- a DBA, a migration outside Weasel -- may have set it deliberately. A table that declares no
parameters at all behaves exactly as it did before this feature existed: no extra SQL, and no delta.

Values compare case-insensitively, and numerically when both sides are numbers. PostgreSQL stores the
text as written, so `0.05` and `0.050` are the same setting and do not produce a permanent false diff.

A difference is an in-place `Update`, written as `ALTER TABLE ... SET (...)`. The rollback restores the
previous value, or `RESET`s a parameter that was not set before.

### Partitioned tables

PostgreSQL rejects storage parameters on a partitioned parent, so Weasel writes them on each partition
instead -- the declared list, range and hash partitions, the default partition, and the ones
`ManagedRangePartitions` adds later. A change applies `ALTER TABLE` to every existing partition.

Only the **direct** partitions of the table are inspected, so a sub-partitioned tree is not recursed into.

### Limitations

`toast.*` parameters are rejected with an exception. They are real parameters, but they live on the TOAST
relation's own `reloptions` rather than the table's, so Weasel could write them and would then read back
nothing and report a difference forever.

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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L176-L192' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_table_delta_detection' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlTableSamples.cs#L197-L204' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_table_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
