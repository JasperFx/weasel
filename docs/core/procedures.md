# Stored Procedures

Stored procedures work on the four engines that have them. SQLite does not — it has no such
concept, and `Weasel.Sqlite.Functions` registers connection-scoped functions rather than modelling
a schema object.

## You supply the whole statement

```csharp
var procedure = new StoredProcedure("audit.stamp", @"
CREATE OR REPLACE PROCEDURE audit.stamp(n int) LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO audit.log (note) VALUES ('touched');
END;
$$;");

await procedure.ApplyChangesAsync(connection);
```

The body is the complete `CREATE PROCEDURE …` statement, not just the procedure's contents. That
is how [functions](/postgresql/functions) already work, and it is what lets you write the
parameter list, the language clause and whatever option flags your engine supports without Weasel
modelling any of it.

`StoredProcedureBase` in `Weasel.Core` owns the shared parts: the statement, the removal flag,
canonicalization for comparison, and the delta shape. Each provider supplies its drop statement,
its catalog query, and whatever its engine does to the statement on the way in.

## Comparison is against what the catalog actually stores

This is where the engines differ, and where a careless implementation reports drift forever.

| Provider | Compared against | The catalog stores |
| --- | --- | --- |
| PostgreSQL | `pg_proc.prosrc` | the body verbatim, between the dollar quotes |
| SQL Server | `sys.sql_modules.definition` | the statement verbatim |
| Oracle | `all_source`, joined by line | the source from `PROCEDURE` onward, without the schema qualifier |
| MySQL | `information_schema.ROUTINES.action_statement` | the body from `BEGIN` onward |

Two of those needed measuring rather than assuming:

**PostgreSQL does not store the header.** `pg_get_functiondef` *renders* it — `(n int, tag text)`
comes back as `(IN n integer, IN tag text)`, and `$$` becomes `$procedure$`. Comparing against that
would report a change on every check for any procedure not written in PostgreSQL's own spelling.
So Weasel compares `prosrc`, and takes your body from between the outermost dollar quotes, which is
unambiguous by construction.

**Oracle stores neither the `CREATE OR REPLACE` wrapper nor the schema qualifier.** A statement
reading `CREATE OR REPLACE PROCEDURE WEASEL.sp_stamp IS` comes back as `PROCEDURE\t sp_stamp IS` —
tab included. Both are stripped before comparing, and any run of whitespace collapses to one space.

::: warning PostgreSQL overloads on the signature
Changing a procedure's **parameter list** does not change that procedure — PostgreSQL creates a
second one and leaves the first in place. Weasel reports the new signature as `Create`; the old
procedure is still there and still yours to drop.
:::

## Applying a change

| Provider | How |
| --- | --- |
| PostgreSQL | `CREATE OR REPLACE PROCEDURE` |
| Oracle | `CREATE OR REPLACE PROCEDURE` |
| SQL Server | `CREATE OR ALTER PROCEDURE`, via `WriteCreateOrAlterStatement` |
| MySQL | drop, then create — it has no replace form |

Oracle's delta emits only the `CREATE OR REPLACE`, because its drop has to be an anonymous PL/SQL
block and ODP.NET cannot execute a block and a DDL statement as one command.

## MySQL and semicolons

A procedure body is full of them, and MySQL's migrator used to split delta SQL on semicolons and
execute the fragments — which shredded every `BEGIN … END` block it saw. It no longer splits;
MySqlConnector executes several statements from one command perfectly well. Fixed in
[#452](https://github.com/JasperFx/weasel/issues/452) for triggers, which have the same shape.
