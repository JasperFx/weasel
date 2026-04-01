# Tables

The `Table` class in `Weasel.SqlServer.Tables` provides a fluent API for defining SQL Server tables, including columns, primary keys, foreign keys, indexes, and partitioning.

## Defining a Table

<!-- snippet: sample_ss_define_table -->
<a id='snippet-sample_ss_define_table'></a>
```cs
var table = new Table("dbo.users");

table.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
table.AddColumn<DateTime>("created_at").DefaultValueByExpression("GETUTCDATE()");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L53-L60' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_define_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Column Configuration

The `AddColumn` method returns a `ColumnExpression` with a fluent API:

- `AsPrimaryKey()` -- marks the column as part of the primary key
- `AutoNumber()` -- adds IDENTITY to the column
- `NotNull()` / `AllowNulls()` -- controls nullability
- `DefaultValue(value)` -- sets a default value (int, long, double, or string)
- `DefaultValueByExpression(expr)` -- sets a default using a SQL expression
- `DefaultValueFromSequence(sequence)` -- default from a named sequence
- `AddIndex(configure?)` -- adds an index on this column
- `ForeignKeyTo(table, column)` -- adds a foreign key constraint

## Foreign Keys

<!-- snippet: sample_ss_foreign_keys -->
<a id='snippet-sample_ss_foreign_keys'></a>
```cs
var orders = new Table("dbo.orders");
orders.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
orders.AddColumn<int>("user_id").NotNull()
    .ForeignKeyTo("dbo.users", "id", onDelete: Weasel.SqlServer.CascadeAction.Cascade);
orders.AddColumn<decimal>("total").NotNull();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L65-L71' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_foreign_keys' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Indexes

<!-- snippet: sample_ss_indexes -->
<a id='snippet-sample_ss_indexes'></a>
```cs
var index = new IndexDefinition("ix_users_email")
{
    Columns = new[] { "email" },
    IsUnique = true,
    IsClustered = false,
    Predicate = "email IS NOT NULL"  // filtered index
};
table.Indexes.Add(index);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L78-L87' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_indexes' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Indexes support `IncludedColumns`, `FillFactor`, `SortOrder`, and `IsClustered` properties.

## Delta Detection

Compare the expected table definition against the actual database state:

<!-- snippet: sample_ss_delta_detection -->
<a id='snippet-sample_ss_delta_detection'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

var delta = await table.FindDeltaAsync(conn);
// delta.Difference is None, Create, Update, or Recreate
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L95-L101' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_ss_generate_ddl -->
<a id='snippet-sample_ss_generate_ddl'></a>
```cs
var migrator = new SqlServerMigrator();
var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);
Console.WriteLine(writer.ToString());
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L108-L113' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
