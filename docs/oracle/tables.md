# Tables

The `Table` class in `Weasel.Oracle.Tables` provides a fluent API for defining Oracle tables with columns, primary keys, foreign keys, indexes, and partitioning.

## Defining a Table

<!-- snippet: sample_oracle_define_table -->
<a id='snippet-sample_oracle_define_table'></a>
```cs
var table = new Table("WEASEL.users");

table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
table.AddColumn<DateTime>("created_at");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L41-L48' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_define_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Column Configuration

The `AddColumn` method returns a `ColumnExpression` with these options:

- `AsPrimaryKey()` -- marks the column as part of the primary key
- `AutoNumber()` -- marks the column as auto-incrementing
- `NotNull()` / `AllowNulls()` -- controls nullability
- `DefaultValue(value)` -- sets a default value
- `DefaultValueByExpression(expr)` -- sets a default using a SQL expression
- `DefaultValueFromSequence(sequence)` -- sets default to `sequence.NEXTVAL`
- `AddIndex(configure?)` -- adds an index on this column
- `ForeignKeyTo(table, column)` -- adds a foreign key constraint

## Foreign Keys

<!-- snippet: sample_oracle_foreign_keys -->
<a id='snippet-sample_oracle_foreign_keys'></a>
```cs
var orders = new Table("WEASEL.orders");
orders.AddColumn<int>("id").AsPrimaryKey();
orders.AddColumn<int>("user_id").NotNull()
    .ForeignKeyTo("WEASEL.users", "id", onDelete: CascadeAction.Cascade);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L53-L58' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_foreign_keys' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Partitioning

Oracle tables support Range, Hash, and List partitioning:

<!-- snippet: sample_oracle_partitioning -->
<a id='snippet-sample_oracle_partitioning'></a>
```cs
table.PartitionByRange("created_at");
table.PartitionByHash("id");
table.PartitionByList("region");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L65-L69' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

<!-- snippet: sample_oracle_delta_detection -->
<a id='snippet-sample_oracle_delta_detection'></a>
```cs
await using var conn = new OracleConnection(connectionString);
await conn.OpenAsync();

var delta = await table.FindDeltaAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L77-L82' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

Oracle DDL uses PL/SQL blocks for safe `CREATE IF NOT EXISTS` semantics:

<!-- snippet: sample_oracle_generate_ddl -->
<a id='snippet-sample_oracle_generate_ddl'></a>
```cs
var migrator = new OracleMigrator();
var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L89-L93' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The generated DDL checks `all_tables` before creating the table, wrapping the logic in a PL/SQL anonymous block.
