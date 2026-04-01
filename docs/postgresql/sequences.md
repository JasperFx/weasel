# Sequences

The `Sequence` class represents a PostgreSQL sequence as a managed schema object.

## Creating a Sequence

<!-- snippet: sample_pg_create_sequence -->
<a id='snippet-sample_pg_create_sequence'></a>
```cs
// Basic sequence
var sequence = new Sequence("public.order_number_seq");

// Sequence with a start value
var sequenceWithStart = new Sequence(
    new DbObjectName("public", "invoice_seq"),
    startWith: 1000);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlSequenceSamples.cs#L12-L20' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_create_sequence' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Owned Sequences

A sequence can be owned by a table column. When the owning table is dropped, the sequence is automatically dropped as well.

<!-- snippet: sample_pg_owned_sequence -->
<a id='snippet-sample_pg_owned_sequence'></a>
```cs
var sequence = new Sequence("public.order_number_seq");
sequence.Owner = new DbObjectName("public", "orders");
sequence.OwnerColumn = "order_number";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlSequenceSamples.cs#L25-L29' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_owned_sequence' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates:

```sql
CREATE SEQUENCE public.order_number_seq;
ALTER SEQUENCE public.order_number_seq OWNED BY public.orders.order_number;
```

## Using with Table Columns

Combine sequences with table column defaults:

<!-- snippet: sample_pg_sequence_with_table_column -->
<a id='snippet-sample_pg_sequence_with_table_column'></a>
```cs
var sequence = new Sequence(
    new DbObjectName("public", "order_number_seq"),
    startWith: 1000);

var table = new Table("orders");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<long>("order_number")
    .DefaultValueFromSequence(sequence);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlSequenceSamples.cs#L34-L43' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_sequence_with_table_column' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Weasel checks whether the sequence exists in the database by querying `information_schema.sequences`. If missing, it returns a `Create` delta.

<!-- snippet: sample_pg_sequence_delta_detection -->
<a id='snippet-sample_pg_sequence_delta_detection'></a>
```cs
var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
var sequence = new Sequence("public.order_number_seq");

await using var conn = dataSource.CreateConnection();
await conn.OpenAsync();

var delta = await sequence.FindDeltaAsync(conn);
// delta.Difference: None or Create
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlSequenceSamples.cs#L48-L57' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_sequence_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_pg_sequence_generate_ddl -->
<a id='snippet-sample_pg_sequence_generate_ddl'></a>
```cs
var sequence = new Sequence("public.order_number_seq");

var migrator = new PostgresqlMigrator();
var writer = new StringWriter();

sequence.WriteCreateStatement(migrator, writer);
// CREATE SEQUENCE public.order_number_seq START 1000;

sequence.WriteDropStatement(migrator, writer);
// DROP SEQUENCE IF EXISTS public.order_number_seq;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlSequenceSamples.cs#L62-L73' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_sequence_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
