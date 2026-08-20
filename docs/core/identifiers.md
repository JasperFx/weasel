# Identifiers and Quoting

What Weasel does with a name you give it — which characters it rejects, which it quotes, and
what it does to your casing. Before 9.25 this was only discoverable by reading each provider's
`SchemaUtils`, and the five had diverged sharply.

## Two rules, and where each one applies

Weasel does two separate things with an identifier, on two different paths, and knowing which
you are on explains almost every surprise:

| | Migration path | Direct API |
| --- | --- | --- |
| Entry point | `ApplyAllConfiguredChangesToDatabaseAsync`, `AssertDatabaseMatchesConfigurationAsync` | `WriteCreateStatement`, `ApplyChangesAsync` |
| Validation | **Strict** — a name that could break out of the statement is rejected | None |
| Quoting | Applied | Applied |

**The migration path validates, because that is where Weasel chooses the names it will bring
into existence.** The direct API does not, because that is how you drive a schema Weasel did not
author — a legacy database whose names you do not control. There, correct quoting is what keeps
a hostile name safe, and rejecting it would only mean you could not use Weasel at all.

## What validation rejects

`Migrator.AssertValidIdentifier` refuses a name when:

| Rejected | Why |
| --- | --- |
| null, empty, all whitespace | there is no object to name |
| leading or trailing whitespace | a typo every time; allowing it silently creates an object nobody meant |
| a line break or a tab | can introduce a `--` comment into an unquoted name |
| `;` | ends the statement and starts another |
| `'` | closes a string literal, and names do reach literals — `IF OBJECT_ID('…')`, `pragma_table_info('…')`, `DEFAULT nextval('…')` |
| the provider's own delimiters | `"` everywhere, `[` and `]` on SQL Server, a backtick on MySQL, a backslash on MySQL |
| longer than the engine's limit | per-provider, see below |

::: tip A plain interior space is allowed
`unit price` is somebody's real legacy column. It cannot smuggle a comment in the way a newline
can, and every provider quotes for shape, so it passes.
:::

### What validation covers

Every name a schema object writes into its DDL:

- the object's own name and the named objects it creates — its indexes and foreign keys
- its columns, its primary key constraint name, its check constraint names

The second group travels through `ISchemaObjectWithLocalIdentifiers` rather than `AllNames()`,
because `AllNames()` yields `DbObjectName` and its callers read a schema off every name it
returns — a column name arriving there would be claiming to be an object in a schema.

## What quoting does, per provider

Every provider now delegates to a shared `IdentifierRules` in `Weasel.Core`. What stays
per-dialect is the delimiter pair, what counts as a regular identifier, and the keyword list.

| | Delimiter | Escapes an embedded delimiter by | Quotes when |
| --- | --- | --- | --- |
| PostgreSQL | `"name"` | doubling `"` | not a regular identifier, a keyword, **or contains an uppercase character** |
| SQL Server | `[name]` | doubling `]` | not a regular identifier, or a keyword |
| Oracle | `"NAME"` | doubling `"` | not a regular identifier, or a keyword |
| MySQL | `` `name` `` | doubling the backtick | **always** |
| SQLite | `"name"` | doubling `"` | not a regular identifier, or a keyword |

Two of those are worth stating outright:

- **PostgreSQL quotes for case as well as for shape.** Leaving `MixedCase` bare would let the
  server fold it, which changes which object the name refers to.
- **MySQL delimits unconditionally**, by design. It is the one provider for which "was this name
  left bare?" is never the right question.

### Casing

| Provider | Unquoted names fold to | Weasel's model holds |
| --- | --- | --- |
| PostgreSQL | lowercase | lowercase, unless `PreserveIdentifierCase` |
| Oracle | UPPERCASE | lowercase, unless `PreserveIdentifierCase` |
| SQLite | nothing (case-insensitive) | lowercase, unless `PreserveIdentifierCase` |
| SQL Server | nothing (case-insensitive) | exactly what you wrote |
| MySQL | nothing | exactly what you wrote |

`PreserveIdentifierCase` controls case folding **and nothing else**. Until 9.25 it also switched
off a space-to-underscore rewrite as a side effect, which meant two unrelated decisions rode on
one flag.

### Names you delimited yourself

A name you have already delimited is passed through rather than escaped again:
`AddColumn("[Order Date]", …)` names the column `Order Date`, not `[Order Date]`.

Weasel emitted most identifiers bare until 9.25, so delimiting a name yourself was the only way
to use one that needed it, and re-escaping those would silently rename the object. The cost is
that a name genuinely containing its own delimiters cannot be expressed — `[x]` names `x`.

### Length limits

| PostgreSQL | SQL Server | Oracle | MySQL | SQLite |
| --- | --- | --- | --- | --- |
| 64 (`NameDataLength`) | 128 | 128 | 64 | 255 (practical cap) |

SQLite has no real limit; 255 is a cap Weasel applies so an accidentally generated name fails
loudly rather than becoming unmanageable. PostgreSQL's is settable via
`PostgresqlMigrator.NameDataLength`, for a server built with a non-default `NAMEDATALEN`.

## Names are never rewritten

A name you supply is either honoured — quoted where it needs quoting — or rejected. It is never
silently changed into a different name.

That was not true before 9.25: three providers turned a space in a *column* name into an
underscore, but only in `TableColumn`. Index, foreign key and primary key column lists were left
alone, so this produced an index over a column that does not exist:

```csharp
table.AddColumn("Order Date", "datetime2");   // was created as Order_Date
table.AddIndex("ix", ["Order Date"]);         // indexed "Order Date"
```

Both halves now say `Order Date`.

::: warning Upgrading
A column declared `"Order Date"` is now created as `"Order Date"` rather than `Order_Date` on
PostgreSQL, Oracle, SQLite and SQL Server. If you were relying on the rewrite, write the
underscore yourself.
:::

## The conformance suite

All of the above is held by cross-provider suites in `Weasel.Core.Tests` rather than described
per provider and hoped for:

- `identifier_rules_conformance` — one set of hostile names (`unit price`, `PK_dbo.__MigrationHistory`, `<Name of Missing Index, sysname,>`, `Grüße`, `2ndPlace`) run against every provider's rules. For any name, a provider either quotes it correctly or leaves it correctly bare, and never emits something that changes which object the name refers to.
- `table_identifier_coverage_conformance` — the union of `AllNames()` and `LocalIdentifiers()` covers every name a table writes.
- `column_name_conformance` — the caller's column name is the column that gets created, and the index, foreign key and primary key lists agree with it.

A sixth provider joins each suite with one entry in its `Providers` table.
