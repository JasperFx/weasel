# Sequences

The `Sequence` class in `Weasel.SqlServer` manages SQL Server sequences with support for delta detection and ownership.

## Defining a Sequence

<!-- snippet: sample_ss_define_sequence -->
<a id='snippet-sample_ss_define_sequence'></a>
```cs
// Simple sequence starting at 1
var seq = new Sequence("dbo.order_seq");

// Sequence with a custom start value
var seq2 = new Sequence(
    DbObjectName.Parse(SqlServerProvider.Instance, "dbo.invoice_seq"),
    startWith: 1000
);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L287-L296' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_define_sequence' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Sequence Ownership

A sequence can be associated with a table column:

<!-- snippet: sample_ss_sequence_ownership -->
<a id='snippet-sample_ss_sequence_ownership'></a>
```cs
seq.Owner = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.orders");
seq.OwnerColumn = "id";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L303-L306' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_sequence_ownership' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Using Sequences with Table Columns

Set a column's default value from a sequence:

<!-- snippet: sample_ss_sequence_with_table -->
<a id='snippet-sample_ss_sequence_with_table'></a>
```cs
var table = new Table("dbo.orders");
var seq = new Sequence("dbo.order_seq");

table.AddColumn<long>("id").AsPrimaryKey()
    .DefaultValueFromSequence(seq);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L311-L317' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_sequence_with_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates: `DEFAULT next value for dbo.order_seq`.

## Generating DDL

<!-- snippet: sample_ss_sequence_ddl -->
<a id='snippet-sample_ss_sequence_ddl'></a>
```cs
var migrator = new SqlServerMigrator();
var writer = new StringWriter();

seq.WriteCreateStatement(migrator, writer);
// Output: IF OBJECT_ID(N'dbo.order_seq', N'SO') IS NULL, then CREATE SEQUENCE dbo.order_seq START WITH 1;

seq.WriteDropStatement(migrator, writer);
// Output: DROP SEQUENCE IF EXISTS dbo.order_seq;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L324-L333' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_sequence_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`CREATE SEQUENCE` has no `IF NOT EXISTS` of its own, so the create statement leads with an
`OBJECT_ID` check for a sequence object (`N'SO'`) of that name. Without it a rendered migration
script fails on its second run with "There is already an object named ... in the database", taking
every statement after it down with it. See
[batch separators and re-runnable scripts](/sqlserver/#batch-separators-and-re-runnable-scripts).

## Delta Detection

Sequences are checked for existence in `sys.sequences`:

<!-- snippet: sample_ss_sequence_delta_detection -->
<a id='snippet-sample_ss_sequence_delta_detection'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

var delta = await seq.FindDeltaAsync(conn);
// delta.Difference: None or Create
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L341-L347' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_sequence_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
