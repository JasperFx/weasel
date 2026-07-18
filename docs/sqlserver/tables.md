# Tables

The `Table` class in `Weasel.SqlServer.Tables` provides a fluent API for defining SQL Server tables, including columns, primary keys, foreign keys, indexes, and partitioning.

## Defining a Table

<!-- snippet: sample_ss_define_table -->
<a id='snippet-sample_ss_define_table'></a>
```cs
var table = new Table("dbo.users");

table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
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
- `ComputedAs(expression, persisted?)` -- makes this a computed column

## Computed Columns

SQL Server computed columns (`[name] AS (expression) [PERSISTED]`) derive their type from the expression — the declared column type is not emitted. The expression and PERSISTED flag are read back from `sys.computed_columns` by `FetchExisting`, and participate in delta detection with canonicalized expression comparison — changing either migrates the column with a lossless drop and re-add (the data is derived). Computed columns the model does not declare are left untouched.

<!-- snippet: sample_ss_computed_columns -->
<a id='snippet-sample_ss_computed_columns'></a>
```cs
var table = new Table("dbo.people");

table.AddColumn<string>("first_name");
table.AddColumn<string>("last_name");

// [full_name] AS (first_name + ' ' + last_name) — the declared type
// is not emitted; SQL Server derives it from the expression
table.AddColumn<string>("full_name")
    .ComputedAs("first_name + ' ' + last_name");

// PERSISTED computed columns are stored on disk and indexable
table.AddColumn<int>("name_length")
    .ComputedAs("len(first_name) + len(last_name)", persisted: true);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L65-L79' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_computed_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Foreign Keys

<!-- snippet: sample_ss_foreign_keys -->
<a id='snippet-sample_ss_foreign_keys'></a>
```cs
var orders = new Table("dbo.orders");
orders.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
orders.AddColumn<int>("user_id").NotNull()
    .ForeignKeyTo("dbo.users", "id", onDelete: Weasel.SqlServer.CascadeAction.Cascade);
orders.AddColumn<decimal>("total").NotNull();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L84-L90' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_foreign_keys' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L97-L106' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_indexes' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L114-L120' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_delta_detection' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L127-L132' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
