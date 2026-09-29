# Functions

The `Function` class in `Weasel.Firebird.Functions` manages a PSQL function (Firebird 3 and later), given as its whole
`CREATE FUNCTION` statement.

## Defining a Function

<!-- snippet: sample_firebird_define_function -->
<a id='snippet-sample_firebird_define_function'></a>
```cs
var function = new Function("fn_order_total", """
    CREATE FUNCTION fn_order_total (order_id INTEGER, discount NUMERIC(5, 2) = 0)
    RETURNS NUMERIC(18, 2)
    AS
        DECLARE VARIABLE total NUMERIC(18, 2);
    BEGIN
        SELECT SUM(amount) FROM order_lines WHERE order_id = :order_id INTO :total;
        RETURN total * (1 - discount);
    END
    """);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L19-L30' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_define_function' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The statement has to name the same function as the identifier. Firebird folds an undelimited name to upper case and
keeps a delimited one as written, so `fn_total` and `"fn_total"` are different functions; a statement that creates
another name throws.

## Generating DDL

| Statement | Written as |
|---|---|
| Create | The statement, its verb rewritten to `CREATE OR ALTER FUNCTION`, inside `SET TERM ^ ;` |
| Update | The same statement: the function is altered in place, never dropped first |
| Drop | An `EXECUTE BLOCK` that runs `DROP FUNCTION` only while the function exists |
| Racing appliers | A `CREATE OR ALTER` that loses a catalog race runs again, so every applier succeeds |

The verb can be `CREATE`, `CREATE OR ALTER`, `RECREATE` or `ALTER`. `CREATE OR ALTER` keeps the views and routines
that call the function, and its grants, through a change of signature. A trailing `;` is left off.

A `^` outside a literal or comment is refused when the statement is written, because it ends a statement in isql.
Write `<>` rather than `^=`.

## Delta Detection

<!-- snippet: sample_firebird_function_delta -->
<a id='snippet-sample_firebird_function_delta'></a>
```cs
await using var conn = new FbConnection(connectionString);
await conn.OpenAsync();

var delta = (CreateOrAlterDelta)await function.FindDeltaAsync(conn);
// delta.Differences names what changed: a parameter, the return type, the body
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L35-L41' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_function_delta' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Firebird keeps the body as source and the rest of the header in `RDB$FUNCTION_ARGUMENTS`, so the whole definition is
compared:

| Part | Compared |
|---|---|
| Parameter names, order and types | Always |
| `NOT NULL`, the default (`=` or `DEFAULT`) | Always; `DEFAULT NULL` is a default |
| Character set, collation, `NUMERIC` precision | When the statement states them |
| A domain, `TYPE OF` a domain, `TYPE OF COLUMN` | By name |
| Return type | As a parameter |
| `DETERMINISTIC` | Always |
| `SQL SECURITY` | Firebird 4 and later |
| Body | Ignoring whitespace and case outside literals |

A type written with a synonym, such as `INT` for `INTEGER`, matches the type it is stored as. `FLOAT(p)` is read for
the server it is compared with. The delta's `Differences` says which part differs.

## Marking for Removal

<!-- snippet: sample_firebird_function_for_removal -->
<a id='snippet-sample_firebird_function_for_removal'></a>
```cs
var removed = Function.ForRemoval("fn_obsolete");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L46-L48' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_function_for_removal' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The migration drops the function and creates nothing.

## Not Modelled

| Object | Why |
|---|---|
| A function in a package | A different object |
| An external (UDR) function | Its body is in a library; the statement is refused |
| A legacy UDF | `DECLARE EXTERNAL FUNCTION`; dropped by teardown only |
