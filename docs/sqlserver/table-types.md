# Table Types

The `TableType` class in `Weasel.SqlServer.Tables` manages user-defined table types, commonly used for table-valued parameters in stored procedures.

## Defining a Table Type

<!-- snippet: sample_ss_define_table_type -->
<a id='snippet-sample_ss_define_table_type'></a>
```cs
var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.OrderItemType");
var tableType = new TableType(identifier);

tableType.AddColumn<int>("product_id").NotNull();
tableType.AddColumn<int>("quantity").NotNull();
tableType.AddColumn("unit_price", "decimal(10,2)");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L335-L342' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_define_table_type' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Column Configuration

`AddColumn` returns an `ITableTypeColumn` that supports:

- `AllowNulls()` -- column accepts NULL values
- `NotNull()` -- column rejects NULL values (default)

You can add columns by .NET type or by explicit database type string:

<!-- snippet: sample_ss_table_type_columns -->
<a id='snippet-sample_ss_table_type_columns'></a>
```cs
tableType.AddColumn<string>("name");            // maps to varchar(100)
tableType.AddColumn("notes", "nvarchar(max)");  // explicit type
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L350-L353' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_table_type_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_ss_table_type_ddl -->
<a id='snippet-sample_ss_table_type_ddl'></a>
```cs
var migrator = new SqlServerMigrator();
var writer = new StringWriter();
tableType.WriteCreateStatement(migrator, writer);
// Output: CREATE TYPE dbo.OrderItemType AS TABLE (product_id int NOT NULL, ...)
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L361-L366' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_table_type_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

`TableTypeDelta` compares expected and actual column definitions by querying `sys.table_types` and `sys.columns`:

<!-- snippet: sample_ss_table_type_delta_detection -->
<a id='snippet-sample_ss_table_type_delta_detection'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

var delta = await tableType.FindDeltaAsync(conn);
// delta.Difference: None, Create, or Update
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L375-L381' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_table_type_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

When an update is needed, the delta generates a DROP followed by CREATE since SQL Server does not support `ALTER TYPE`.

## Using with Stored Procedures

Table types enable passing structured data to stored procedures:

```sql
CREATE PROCEDURE dbo.InsertOrderItems
    @Items dbo.OrderItemType READONLY
AS
BEGIN
    INSERT INTO order_items (product_id, quantity, unit_price)
    SELECT product_id, quantity, unit_price FROM @Items;
END;
```
