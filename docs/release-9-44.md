# Upgrading to 9.44

One change: a SQL Server function's creation script can be run more than once.

## SQL Server functions in a creation script survive a re-run

[#703](https://github.com/JasperFx/weasel/issues/703) / [#704](https://github.com/JasperFx/weasel/pull/704).
`Weasel.SqlServer.Functions.Function` rendered a bare `CREATE FUNCTION`, so a generated creation
script that contained a function failed on its **second** run:

```
There is already an object named 'my_function' in the database.
```

Every other object in a generated script survives a re-run, and `Trigger` had this exact problem first.
`Function` now gets the same answer: `WriteCreateStatement` drops the function if it exists, then
creates it.

### What changes for you

- **A function's creation script now begins with its drop statement.** If you render scripts and read
  them, you will see `drop function if exists ...;` ahead of each `EXEC sp_executesql`. That matches
  `Trigger`. It would fail for a function that something references `WITH SCHEMABINDING`, which the
  update path (already drop-then-create) would hit anyway.
- **New:** `Function.CreateStatement()` renders the create alone, with no drop, the same way
  `Trigger.CreateStatement()` does.
- **Delta comparison is unchanged in effect**, and that took a second change. `FunctionDelta` used to
  compare `Body()`, and `Body()` renders through `WriteCreateStatement`, so it now includes the drop. A
  function read back from the database always carries the default drop statement, so a function you
  declared with **custom** drop statements would have reported a change on every migration. The delta
  now compares `CreateStatement()` on both sides, and a test pins the custom-drop case.

Nothing public was removed and no signature changed.

## New public API

- `Weasel.SqlServer.Functions.Function.CreateStatement()`
