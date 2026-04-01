# Sequences

The `Sequence` class in `Weasel.Oracle` manages Oracle sequences with safe creation using PL/SQL exception handling.

## Defining a Sequence

<!-- snippet: sample_oracle_define_sequence -->
<a id='snippet-sample_oracle_define_sequence'></a>
```cs
// Simple sequence starting at 1
var seq = new Sequence("WEASEL.order_seq");

// Sequence with a custom start value
var seq2 = new Sequence(
    DbObjectName.Parse(OracleProvider.Instance, "WEASEL.invoice_seq"),
    startWith: 1000
);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L100-L109' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_define_sequence' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Using Sequences with Table Columns

Set a column's default to the sequence's NEXTVAL:

<!-- snippet: sample_oracle_sequence_with_table -->
<a id='snippet-sample_oracle_sequence_with_table'></a>
```cs
var table = new Table("WEASEL.orders");
var seq = new Sequence("WEASEL.order_seq");

table.AddColumn<long>("id").AsPrimaryKey()
    .DefaultValueFromSequence(seq);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L114-L120' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_sequence_with_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates a default expression of `WEASEL.order_seq.NEXTVAL`.

## Generating DDL

The create statement uses PL/SQL exception handling to safely skip creation if the sequence already exists (ORA-00955):

<!-- snippet: sample_oracle_sequence_create_ddl -->
<a id='snippet-sample_oracle_sequence_create_ddl'></a>
```cs
var migrator = new OracleMigrator();
var writer = new StringWriter();
seq.WriteCreateStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L127-L131' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_sequence_create_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The drop statement checks `all_sequences` before dropping:

<!-- snippet: sample_oracle_sequence_drop_ddl -->
<a id='snippet-sample_oracle_sequence_drop_ddl'></a>
```cs
seq.WriteDropStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L140-L142' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_sequence_drop_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Sequences are checked for existence in `all_sequences`:

<!-- snippet: sample_oracle_sequence_delta_detection -->
<a id='snippet-sample_oracle_sequence_delta_detection'></a>
```cs
await using var conn = new OracleConnection(connectionString);
await conn.OpenAsync();

var delta = await seq.FindDeltaAsync(conn);
// delta.Difference: None or Create
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L150-L156' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_sequence_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
