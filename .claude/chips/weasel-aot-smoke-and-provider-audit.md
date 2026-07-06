# Title

Weasel#263: Add Weasel.Core.AotSmoke consumer project + per-provider IsAotCompatible audit

## TL;DR

Add a CI-gated AOT smoke-test project that references Weasel.Core in Static mode and fails the build on IL2026/IL3050 (and siblings). Also do the per-provider AOT-flag audit: confirm IsAotCompatible state on each Weasel.* provider project (Postgresql/SqlServer/Oracle/MySql/Sqlite/EntityFrameworkCore); annotate the wrapper for AOT-hostile ADO.NET surfaces rather than chase upstream providers.

## Prompt

**Repo: Weasel** (https://github.com/JasperFx/weasel). Root: `/Users/jeremymiller/code/weasel`. Base branch: `main`. Open PR against `JasperFx/weasel`.

If your worktree is rooted anywhere other than the Weasel repo, STOP and report — do not attempt cross-repo edits.

Master plan: **Weasel#263** ([Master] Weasel 9.0). This chip ticks off two boxes:
- "For database-specific Weasel.* projects (Postgresql, SqlServer, Oracle, MySql, Sqlite, EntityFrameworkCore): evaluate per-project AOT-cleanness."
- The missing AOT smoke-test consumer project (parallel to `JasperFx.AotSmoke` in jasperfx).

## Context

`Weasel.Core.csproj` already sets `<IsAotCompatible>true</IsAotCompatible>`. The reflective surface in Weasel.Core is clean (no live `MakeGenericType` / `Activator.CreateInstance` outside annotated paths). Migration guide for 8→9 already shipped (#276 / `dd1c81a`).

What's missing: (1) a consumer-side smoke-test project that gates regressions on Weasel.Core's already-AOT-clean state, and (2) the per-provider `Weasel.*` audit — each provider project needs `IsAotCompatible=true` if its surface is clean, OR the AOT-hostile ADO.NET surfaces need `[RequiresDynamicCode]` / `[RequiresUnreferencedCode]` annotations on the Weasel wrapper.

The pattern to mirror is **`src/JasperFx.AotSmoke/`** in the jasperfx repo (commit `d4077d8`). Inspect that project before starting — same shape, just adapted for Weasel.Core's surface.

## Scope

### Part 1 — Weasel.Core.AotSmoke project

1. **New project: `src/Weasel.Core.AotSmoke/Weasel.Core.AotSmoke.csproj`**
   - Console app SDK (`Microsoft.NET.Sdk`), `<OutputType>Exe</OutputType>`.
   - `<IsAotCompatible>true</IsAotCompatible>`, `<TrimMode>full</TrimMode>`.
   - Mirror JasperFx.AotSmoke's `WarningsAsErrors` list: `IL2026;IL2046;IL2055;IL2065;IL2067;IL2070;IL2072;IL2075;IL2090;IL2091;IL2111;IL3050;IL3051`.
   - Target framework matches the rest of the solution.
   - ProjectReference to `Weasel.Core`.
   - **No** reference to any database-specific Weasel.* provider — that's intentionally separate (the per-provider audit in Part 2).

2. **Realistic surface in `Program.cs`:**
   - Exercise a few `Weasel.Core` surfaces: build a simple `Table` definition with columns, get a `CreateStatement` from it, exercise `DdlSyntaxStrategy` (or whatever the AOT-relevant primitives are post-#270 consolidation).
   - Goal: cover the consolidation surfaces from #270 (`SchemaObjectBase`, `TableBase`, `IDdlSyntaxStrategy`, unified `ColumnExpression`, `ForeignKeyBase`).
   - Exit immediately after the surface is touched (`return 0`).

3. **Solution + CI integration:**
   - Add the new project to the Weasel solution (`dotnet sln add`).
   - Verify `.github/workflows/*.yml` builds it as part of the standard build.
   - Don't add it as a test target.

### Part 2 — Per-provider IsAotCompatible audit

For **each** of `Weasel.Postgresql`, `Weasel.SqlServer`, `Weasel.Oracle`, `Weasel.MySql`, `Weasel.Sqlite`, `Weasel.EntityFrameworkCore`:

1. Inspect the provider's csproj for current `IsAotCompatible` state.
2. Try setting `<IsAotCompatible>true</IsAotCompatible>` if not already on.
3. Build the project. If it builds clean → leave the flag on, commit.
4. If it surfaces IL2026/IL2075/IL3050 etc.:
   - If the warnings are from the **Weasel wrapper code itself**, annotate the call sites at the wrapper layer (`[RequiresDynamicCode]` / `[RequiresUnreferencedCode]` with a justification).
   - If the warnings are from **upstream ADO.NET provider code** (e.g. Npgsql, Microsoft.Data.SqlClient, Oracle.ManagedDataAccess), per the issue body: "annotate the Weasel wrapper rather than chase upstream." Add a comment explaining which upstream surface caused the wrapper-level annotation.
   - If a provider is genuinely AOT-hostile through and through, leave `IsAotCompatible` off (or set it but with `<NoWarn>` listing the specific upstream-driven warnings) and add a comment block explaining why.
5. **Document the outcome per provider in the PR body** — a small table:

   | Provider | IsAotCompatible | Notes |
   |---|---|---|
   | Weasel.Postgresql | true | Clean, no annotations needed |
   | Weasel.SqlServer | true (annotated) | `SqlBulkCopy.WriteToServer` requires DynamicCode — annotated at `BulkInserter` |
   | ... | | |

## Out of scope

- Audit reflective call sites *inside* Weasel.Core (already clean per current state).
- IStorageOperation migration (separate, bigger chip).
- Hot-path GenericFactoryCache migration (separate, requires profiling).
- Database-related enum dedupe (separate chip).

## Acceptance

- `dotnet build` clean with the new smoke-test project.
- Per-provider state documented in the PR body's audit table.
- Manually adding an unannotated `MakeGenericType` call somewhere in Weasel.Core surfaces a warning when the smoke-test compiles (revert before commit, note in PR body).
- The smoke-test project's `Program.cs` exits cleanly when run.

## Commit shape

2-4 commits in one PR:
1. `Add Weasel.Core.AotSmoke project + Static-mode bootstrap`
2. `Audit IsAotCompatible across Weasel.* provider projects`
3. (per-provider, optional) `Annotate Weasel.<Provider> wrapper for AOT-hostile upstream surfaces`

## PR

Title: `Weasel#263: Add Weasel.Core.AotSmoke + per-provider IsAotCompatible audit`
Body: include the per-provider audit table. Reference `#263` (not "Closes" — master plan).

## Don't

- Don't pull in JasperFx.RuntimeCompiler.
- Don't make this an xUnit test project.
- Don't lower `WarningsAsErrors` in the new project's csproj.
- Don't modify upstream ADO.NET provider code — annotate at the Weasel wrapper layer per the master plan's explicit guidance.
- Don't make cross-repo edits.
