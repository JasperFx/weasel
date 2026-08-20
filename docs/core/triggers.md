# Triggers

Triggers work on all five providers as of 9.25. They were the one whole category of database
object that no provider modelled, and the one with the most variation between engines — so the
shared model is deliberately small and each provider refuses what it cannot express rather than
quietly narrowing it.

## A trigger is not owned by its table

```csharp
var trigger = new Trigger("audit.stamp_orders", "audit.orders", body)
{
    Timing = TriggerTiming.Before,
    Events = TriggerEvents.Insert | TriggerEvents.Update
};

await trigger.ApplyChangesAsync(connection);
```

Indexes and foreign keys hang off `Table`. Triggers do not — they are independent schema objects
that *declare* a target. Three reasons:

- **A trigger does not always have a table.** SQL Server's `INSTEAD OF` triggers attach to views;
  Oracle has schema- and database-level triggers with no target object at all.
- **A PostgreSQL trigger calls a function**, so it depends on another schema object rather than on
  its table.
- **Indexes and foreign keys are part of the table's own definition** and several are emitted
  inside `CREATE TABLE`. A trigger is always a separate statement with its own lifecycle.

The cost, stated plainly: a trigger and its target can drift apart in your model, because nothing
forces you to register them together. Views and functions already behave this way.

::: warning SQLite rebuilds a table by dropping it
`DROP TABLE` takes the table's triggers with it and says nothing. Weasel's SQLite table rebuild
captures the triggers from `sqlite_master` first and re-emits them afterwards — including triggers
Weasel never declared, because a hand-written trigger is still yours.
:::

## What each engine accepts

| | PostgreSQL | SQL Server | Oracle | MySQL | SQLite |
| --- | --- | --- | --- | --- | --- |
| `BEFORE` | ✓ | — | ✓ | ✓ | ✓ |
| `AFTER` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `INSTEAD OF` | ✓ (views) | ✓ (views) | ✓ | — | ✓ (views) |
| Several events on one trigger | ✓ | ✓ | ✓ | — | — |
| `TRUNCATE` event | ✓ | — | — | — | — |
| Row-level (`FOR EACH ROW`) | ✓ | — always statement | ✓ | always | always |
| `WHEN` condition | ✓ | — | ✓ | — | ✓ |
| Body | a function call | T-SQL | PL/SQL | SQL | SQL |

**An engine that cannot express something refuses it.** Setting `Condition` on SQL Server throws
and names the alternative; asking MySQL for two events throws and tells you to declare two
triggers. This is the rule settled in [#449](https://github.com/JasperFx/weasel/issues/449) after
two index properties were found to be silently ignored — a caller who sets a property gets it, or
gets an exception, never a quietly narrower object.

## PostgreSQL composes with a function

PostgreSQL triggers have no body. They execute a function, which must exist first, so
`Trigger.Body` is the call:

```csharp
var function = new Function(new PostgresqlObjectName("audit", "stamp_note"), @"
CREATE OR REPLACE FUNCTION audit.stamp_note() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.note := 'touched';
    RETURN NEW;
END;
$$;");

var trigger = new Trigger("audit.stamp_note_trigger", "audit.orders", "audit.stamp_note()");
```

Register both. Nothing enforces the ordering for you, which is the same trade-off as the target.

## MySQL needs more than the TRIGGER privilege

Creating a trigger needs `TRIGGER`, and on a server with binary logging enabled it also needs
`SUPER` or `log_bin_trust_function_creators`. MySQL refuses otherwise with a message about the
SUPER privilege that never mentions triggers, which is worth recognising:

```
You do not have the SUPER privilege and binary logging is enabled
```

## Delta detection

Every provider compares the trigger it would create against what the catalog holds, ignoring
whitespace and case:

| Provider | Read from | Stored |
| --- | --- | --- |
| PostgreSQL | `pg_get_triggerdef` | rendered by the server |
| SQL Server | `sys.sql_modules` | verbatim |
| Oracle | `all_triggers.trigger_body` | verbatim |
| MySQL | `information_schema.TRIGGERS.action_statement` | verbatim |
| SQLite | `sqlite_master.sql` | verbatim |

MySQL is the interesting one: it rewrites a *view* definition but stores a *trigger* body as
submitted, so triggers need none of the probe machinery
[views do](/core/object-types).

Only Oracle has `CREATE OR REPLACE TRIGGER`. The other four drop and recreate, which their
`WriteCreateStatement` does in one go — so applying a trigger is idempotent everywhere.
