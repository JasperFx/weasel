# Triggers

The `Trigger` class in `Weasel.Firebird.Triggers` manages a trigger on a table or a view. `Body` is the PSQL after `AS`:
a `BEGIN … END` block, with any `DECLARE VARIABLE` lines before it.

## Defining a Trigger

<!-- snippet: sample_firebird_define_trigger -->
<a id='snippet-sample_firebird_define_trigger'></a>
```cs
var trigger = new Trigger("trg_orders_stamp", "orders", """
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        IF (INSERTING) THEN NEW.created_at = CURRENT_TIMESTAMP;
    END
    """)
{
    Timing = TriggerTiming.Before,
    Events = TriggerEvents.Insert | TriggerEvents.Update
};
// CREATE OR ALTER TRIGGER trg_orders_stamp FOR orders ACTIVE BEFORE INSERT OR UPDATE AS ...
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L85-L97' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_define_trigger' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A body that is neither a block nor starts with its declarations is wrapped in `BEGIN … END`.

## Generating DDL

| Statement | Written as |
|---|---|
| Create | `CREATE OR ALTER TRIGGER name FOR table ACTIVE timing events AS body`, inside `SET TERM ^ ;` |
| Update | The same statement: it changes the table, timing, events and body in place |
| Drop | An `EXECUTE BLOCK` that runs `DROP TRIGGER` only while the trigger exists |
| Racing appliers | A `CREATE OR ALTER` that loses a catalog race runs again, so every applier succeeds |

`ACTIVE` is written out: `CREATE OR ALTER` otherwise leaves an inactive trigger inactive.

| Property | Firebird |
|---|---|
| `Timing` | `Before` or `After`. `InsteadOf` throws `NotSupportedException` |
| `Events` | `Insert`, `Update`, `Delete`, any combination, written `INSERT OR UPDATE OR DELETE`. `Truncate` throws |
| `Condition` | Throws: Firebird has no `WHEN` clause. Test the condition in the body |
| `ForEachRow` | Not emitted: Firebird triggers are row-level |
| `PreserveTargetCase` | Names the table exactly, delimited, for a case-preserved (EF Core) table. Copied from `PreserveIdentifierCase` when the constructor is given a `Table` |

## Triggers on Views

Firebird has no `INSTEAD OF`. A `BEFORE` trigger on a view does the same job:

<!-- snippet: sample_firebird_trigger_on_view -->
<a id='snippet-sample_firebird_trigger_on_view'></a>
```cs
var trigger = new Trigger("trg_order_notes_insert", "order_notes", """
    BEGIN
        INSERT INTO orders (id, note) VALUES (NEW.id, NEW.note);
    END
    """)
{
    Timing = TriggerTiming.Before,
    Events = TriggerEvents.Insert
};
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdPsqlSamples.cs#L102-L112' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_trigger_on_view' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Firebird keeps the body as source, `AS` included, and the rest in `RDB$TRIGGERS`. All of it is compared:

| Part | Change? |
|---|---|
| The table or view it fires on | Yes |
| Timing | Yes |
| Events | Yes; their order is not: `UPDATE OR INSERT` is `INSERT OR UPDATE` |
| Switched off with `ALTER TRIGGER … INACTIVE` | Yes; the update makes it active again |
| Body | Ignoring whitespace and case outside literals |

## Not Modelled

Database triggers (`ON CONNECT`, `ON TRANSACTION …`) and DDL triggers, as on MySQL and Oracle. `POSITION` is neither
written nor compared.

## Teardown

Dropping a table drops its triggers. `DropSchemaAsync` drops the rest, triggers on views included.
