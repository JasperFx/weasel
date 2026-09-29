# Stored Procedures

The `StoredProcedure` class in `Weasel.Firebird.Procedures` manages a PSQL procedure, executable or selectable, given as
its whole `CREATE PROCEDURE` statement.

## Defining a Stored Procedure

<!-- snippet: sample_firebird_define_procedure -->
<a id='snippet-sample_firebird_define_procedure'></a>
```cs
var procedure = new StoredProcedure("sp_recent_orders", """
    CREATE PROCEDURE sp_recent_orders (since TIMESTAMP, max_rows INTEGER = 10)
    RETURNS (id INTEGER, placed_at TIMESTAMP)
    AS
    BEGIN
        FOR SELECT FIRST :max_rows id, placed_at FROM orders
            WHERE placed_at >= :since ORDER BY placed_at DESC
            INTO :id, :placed_at
        DO SUSPEND;
    END
    """);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L55-L67' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_define_procedure' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The statement has to name the same procedure as the identifier. `sp_orders` and `"sp_orders"` are different
procedures; a statement that creates another name throws.

## Generating DDL

| Statement | Written as |
|---|---|
| Create | The statement, its verb rewritten to `CREATE OR ALTER PROCEDURE`, inside `SET TERM ^ ;` |
| Update | The same statement: the procedure is altered in place, never dropped first |
| Drop | An `EXECUTE BLOCK` that runs `DROP PROCEDURE` only while the procedure exists |
| Racing appliers | A `CREATE OR ALTER` that loses a catalog race runs again, so every applier succeeds |

`CREATE OR ALTER` keeps the procedures, views and triggers that call the procedure, and its grants, through a change
of parameters. A `^` outside a literal or comment is refused, because it ends a statement in isql.

Set `IsRemoved` to drop the procedure and create nothing.

## Delta Detection

Firebird keeps the body as source and the parameters in `RDB$PROCEDURE_PARAMETERS`, so the whole definition is
compared:

| Part | Compared |
|---|---|
| Input and output parameter names, order and types | Always |
| `NOT NULL`, an input's default (`=` or `DEFAULT`) | Always; `DEFAULT NULL` is a default |
| Character set, collation, `NUMERIC` precision | When the statement states them |
| A domain, `TYPE OF` a domain, `TYPE OF COLUMN` | By name |
| `SQL SECURITY` | Firebird 4 and later |
| Body | Ignoring whitespace and case outside literals |

Whether a procedure is selectable is not compared: Firebird decides it from `SUSPEND` in the body.

## Fetching Existing Definitions

<!-- snippet: sample_firebird_procedure_fetch_existing -->
<a id='snippet-sample_firebird_procedure_fetch_existing'></a>
```cs
await using var conn = new FbConnection(connectionString);
await conn.OpenAsync();

var existing = await procedure.FetchExistingAsync(conn);
// existing.BodyText() is a CREATE OR ALTER PROCEDURE statement rebuilt from the catalog
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L72-L78' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_procedure_fetch_existing' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The statement is rebuilt from the catalog with every name delimited, so running it recreates the same procedure. A
rollback runs it.

## Not Modelled

A procedure in a package, and an external (UDR) procedure, are different objects.
