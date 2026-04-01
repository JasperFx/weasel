# Tables

The `Table` class in `Weasel.MySql.Tables` provides a fluent API for defining MySQL tables with columns, primary keys, foreign keys, indexes, and partitioning.

## Defining a Table

<!-- snippet: sample_mysql_define_table -->
<a id='snippet-sample_mysql_define_table'></a>
```cs
var table = new Table("users");

table.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
table.AddColumn<DateTime>("created_at");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L42-L49' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_define_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Table Options

MySQL tables support engine, character set, and collation configuration:

<!-- snippet: sample_mysql_table_options -->
<a id='snippet-sample_mysql_table_options'></a>
```cs
table.Engine = "InnoDB";       // default
table.Charset = "utf8mb4";
table.Collation = "utf8mb4_unicode_ci";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L56-L60' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_table_options' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Column Configuration

The `AddColumn` method returns a `ColumnExpression` with these options:

- `AsPrimaryKey()` -- marks the column as part of the primary key
- `AutoNumber()` -- adds AUTO_INCREMENT to the column
- `NotNull()` / `AllowNulls()` -- controls nullability
- `DefaultValue(value)` -- sets a default value
- `DefaultValueByExpression(expr)` -- sets a default using a SQL expression
- `AddIndex(configure?)` -- adds a standard index
- `AddFulltextIndex(configure?)` -- adds a FULLTEXT index
- `AddSpatialIndex(configure?)` -- adds a SPATIAL index
- `ForeignKeyTo(table, column)` -- adds a foreign key constraint

## Partitioning

MySQL tables support Range, Hash, List, and Key partitioning:

<!-- snippet: sample_mysql_partitioning -->
<a id='snippet-sample_mysql_partitioning'></a>
```cs
table.PartitionByRange("created_at");
table.PartitionByHash("id");
table.PartitionByList("region");
table.PartitionByKey("id");
table.PartitionCount = 4;  // for Hash or Key strategies
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L67-L73' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

<!-- snippet: sample_mysql_delta_detection -->
<a id='snippet-sample_mysql_delta_detection'></a>
```cs
await using var conn = new MySqlConnection(connectionString);
await conn.OpenAsync();

var delta = await table.FindDeltaAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L81-L86' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_mysql_generate_ddl -->
<a id='snippet-sample_mysql_generate_ddl'></a>
```cs
var migrator = new MySqlMigrator();
var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L93-L97' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The output uses `CREATE TABLE IF NOT EXISTS` with backtick-quoted identifiers.
