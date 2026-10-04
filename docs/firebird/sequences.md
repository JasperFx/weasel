# Sequences

The `Sequence` class in `Weasel.Firebird` manages a native Firebird sequence (a generator), created with a guarded
`CREATE SEQUENCE`.

## Defining a Sequence

<!-- snippet: sample_firebird_define_sequence -->
<a id='snippet-sample_firebird_define_sequence'></a>
```cs
// Starts at 1 and counts up by 1
var seq = new Sequence("order_seq");

// The first value is 1000 on Firebird 3, 4 and 5 alike
var invoices = new Sequence("invoice_seq") { StartWith = 1000, IncrementBy = 10 };
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L131-L137' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_define_sequence' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`StartWith` is the first value `NEXT VALUE FOR` returns, on every version. Firebird 3 reads `START WITH` as the
current value, so its first value is one increment past it; Firebird 4 and later hand it out first. The create asks
the engine which it is and writes the value each one needs.

## Generating DDL

<!-- snippet: sample_firebird_sequence_create_ddl -->
<a id='snippet-sample_firebird_sequence_create_ddl'></a>
```cs
var migrator = new FirebirdMigrator();
var writer = new StringWriter();
seq.WriteCreateStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L144-L148' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_sequence_create_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates:

```sql
SET TERM ^ ;
EXECUTE BLOCK AS
BEGIN
  IF (NOT EXISTS(SELECT 1 FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = 'INVOICE_SEQ')) THEN
    IF (rdb$get_context('SYSTEM', 'ENGINE_VERSION') STARTING WITH '3.') THEN
      EXECUTE STATEMENT 'CREATE SEQUENCE invoice_seq START WITH 990 INCREMENT BY 10';
    ELSE
      EXECUTE STATEMENT 'CREATE SEQUENCE invoice_seq START WITH 1000 INCREMENT BY 10';
END
^
SET TERM ; ^
```

A sequence with neither option is a plain guarded `CREATE SEQUENCE`. Weasel never writes `CREATE OR ALTER SEQUENCE`:
it is a syntax error without an option, and with one it would reset a sequence already in use.

To drop:

<!-- snippet: sample_firebird_sequence_drop_ddl -->
<a id='snippet-sample_firebird_sequence_drop_ddl'></a>
```cs
seq.WriteDropStatement(migrator, writer);
// An EXECUTE BLOCK that runs DROP SEQUENCE invoice_seq only while it exists
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L157-L160' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_sequence_drop_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

The sequence is read from `RDB$GENERATORS`:

<!-- snippet: sample_firebird_sequence_delta_detection -->
<a id='snippet-sample_firebird_sequence_delta_detection'></a>
```cs
await using var conn = new FbConnection(connectionString);
await conn.OpenAsync();

var delta = await seq.FindDeltaAsync(conn);
// Create when it is missing, Update when its increment differs, otherwise None
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L169-L175' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_sequence_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

| Model | Catalog | Result |
|---|---|---|
| any | missing | `Create` |
| `IncrementBy` stated, different | any | `Update`: `ALTER SEQUENCE … INCREMENT BY`, keeping the value reached |
| `IncrementBy` not stated, or the same | any | `None` |

The start value is not compared: the catalog keeps where a sequence has got to, not where it began. Dropping and
recreating a sequence would restart it and hand out values already used, so no delta does that.

## Usage Pattern

To get the next value in application code:

```sql
SELECT NEXT VALUE FOR invoice_seq FROM RDB$DATABASE;
```

For a key column, `AutoIncrement()` is usually simpler: an identity column has a sequence of its own, which Firebird
creates and drops with the column.
