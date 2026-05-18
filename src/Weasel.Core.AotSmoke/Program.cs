// AOT smoke test (weasel#263 / JasperFx/jasperfx#213).
//
// This program touches a representative cross-section of the AOT-clean
// Weasel.Core surface. The csproj sets IsAotCompatible=true, TrimMode=full,
// and promotes the AOT analyzer warning codes to errors, so any change that
// adds [RequiresDynamicCode] / [RequiresUnreferencedCode] to an API
// exercised here — or any change to this file that calls into a reflective
// Weasel.Core surface — fails the build in CI.
//
// The consolidation surfaces from #270 are the primary target:
//   - SchemaObjectBase / SequenceBase / FunctionBase / ViewBase (covered
//     indirectly via TableBase, which extends SchemaObjectBase)
//   - TableBase<TColumn, TIndex, TForeignKey>
//   - ForeignKeyBase (Parse, LinkColumns, Equals/GetHashCode)
//   - IDdlSyntaxStrategy
//   - DbObjectName, CascadeAction, EnumStorage, CreationStyle,
//     SchemaPatchDifference, SqlFormatting
//
// Intentionally *not* exercised here (those carry AOT annotations by design):
//   - AssertCommand.Execute is [RequiresDynamicCode] (Spectre.Console
//     ExceptionFormatter dependency).
//   - CommandBuilderBase.AddParameters(object) wraps an unconditional
//     IL2075 suppression for the parameters→GetType()→GetProperties chain
//     (the parameter is annotated [DynamicallyAccessedMembers] as the
//     caller-facing contract).

using System.Data.Common;
using JasperFx;
using Weasel.Core;
using Weasel.Core.Migrations;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

// --- DbObjectName --------------------------------------------------------
// Pure value object; ToString / QualifiedName should be deterministic.

var id = new DbObjectName("public", "smoke_test");
if (id.QualifiedName != "public.smoke_test")
{
    Console.Error.WriteLine($"DbObjectName.QualifiedName regression: {id.QualifiedName}");
    return 1;
}

// --- Enums round-trip ---------------------------------------------------
// Touch each enum surface that consumers commonly use.

if (CascadeAction.Cascade.ToString() != "Cascade" ||
    EnumStorage.AsInteger.ToString() != "AsInteger" ||
    CreationStyle.CreateIfNotExists.ToString() != "CreateIfNotExists" ||
    SchemaPatchDifference.None.ToString() != "None" ||
    SqlFormatting.Concise.ToString() != "Concise" ||
    BulkInsertMode.InsertsOnly.ToString() != "InsertsOnly" ||
    AutoCreate.None.ToString() != "None")
{
    Console.Error.WriteLine("Enum ToString regression.");
    return 1;
}

// --- ForeignKeyBase shared parsing helpers ------------------------------
// ForeignKeyBase.Parse and ParseCascadeClause are consolidation outputs
// of #270 step 4. Exercise via a minimal subclass.

var fk = new SmokeForeignKey("smoke_fkey");
fk.Parse("FOREIGN KEY (state_id) REFERENCES states(id) ON DELETE CASCADE ON UPDATE NO ACTION");
if (fk.ColumnNames.Length != 1 || fk.ColumnNames[0].Trim() != "state_id" ||
    fk.LinkedNames.Length != 1 || fk.LinkedNames[0].Trim() != "id" ||
    fk.LinkedTable?.QualifiedName != "smoke.states" ||
    fk.DeleteAction != CascadeAction.Cascade ||
    fk.UpdateAction != CascadeAction.NoAction)
{
    Console.Error.WriteLine($"ForeignKeyBase.Parse regression: " +
                            $"cols={string.Join(",", fk.ColumnNames)} " +
                            $"linked={string.Join(",", fk.LinkedNames)} " +
                            $"table={fk.LinkedTable?.QualifiedName} " +
                            $"del={fk.DeleteAction} upd={fk.UpdateAction}");
    return 1;
}

// LinkColumns appends; structural Equals across same-provider-root FKs.
var fk2 = new SmokeForeignKey("smoke_fkey");
fk2.LinkColumns("state_id", "id");
fk2.LinkedTable = new DbObjectName("smoke", "states");
fk2.DeleteAction = CascadeAction.Cascade;
fk2.UpdateAction = CascadeAction.NoAction;
if (!fk.Equals(fk2))
{
    Console.Error.WriteLine("ForeignKeyBase.Equals regression: structurally-equal FKs compared unequal.");
    return 1;
}

// --- TableBase consolidation surface ------------------------------------
// Build a Table via the abstract base, exercise the column / index / PK
// helpers and the explicit ITable interface implementations.

ITable table = new SmokeTable(id);
table.AddColumn("id", typeof(int));
table.AddPrimaryKeyColumn("tenant_id", typeof(string));
var added = (table as SmokeTable)!;
if (!added.HasColumn("id") || added.ColumnFor("tenant_id") is null)
{
    Console.Error.WriteLine("TableBase HasColumn/ColumnFor regression.");
    return 1;
}

// PrimaryKeyName auto-default via DefaultPrimaryKeyName hook.
if (added.PrimaryKeyName != "pk_smoke_test_tenant_id")
{
    Console.Error.WriteLine($"TableBase.PrimaryKeyName regression: {added.PrimaryKeyName}");
    return 1;
}

// ITable.AddForeignKey routes through the abstract CreateForeignKey hook.
var fkBase = table.AddForeignKey("fk_smoke_states", new DbObjectName("smoke", "states"),
    new[] { "state_id" }, new[] { "id" });
if (fkBase.Name != "fk_smoke_states")
{
    Console.Error.WriteLine("ITable.AddForeignKey regression.");
    return 1;
}

// RemoveColumn is case-insensitive on every provider.
added.RemoveColumn("ID");
if (added.HasColumn("id"))
{
    Console.Error.WriteLine("TableBase.RemoveColumn (case-insensitive) regression.");
    return 1;
}

added.IgnoreIndex("smoke_ignored_idx");
if (!added.HasIgnoredIndex("smoke_ignored_idx"))
{
    Console.Error.WriteLine("TableBase.IgnoreIndex regression.");
    return 1;
}

// --- IDdlSyntaxStrategy --------------------------------------------------
// #270 step 8 — pluggable per-provider DDL syntax decisions. Exercise the
// interface via a minimal implementation; consumer code must be able to
// call the strategy methods without AOT-hostile reflection.

IDdlSyntaxStrategy syntax = new SmokeSyntax();
var w = new StringWriter();
syntax.WriteDropTable(w, id);
syntax.WriteCreateTableHeader(w, id, CreationStyle.CreateIfNotExists);
if (syntax.QuoteIdentifier("x") != "\"x\"" ||
    syntax.InlineForeignKeyConstraints ||
    syntax.AutoIncrementToken != "SMOKE_INCR" ||
    syntax.StatementTerminator != ";")
{
    Console.Error.WriteLine("IDdlSyntaxStrategy regression.");
    return 1;
}

Console.WriteLine($"Weasel.Core AOT smoke OK — exercised {nameof(DbObjectName)}, " +
                  $"{nameof(ForeignKeyBase)}.Parse, {nameof(TableBase<SmokeColumn, SmokeIndex, SmokeForeignKey>)}, " +
                  $"{nameof(IDdlSyntaxStrategy)}.");
return 0;


// ===========================================================================
// Minimal stubs — implement just enough of each abstract base to instantiate
// it from this consumer project. Bodies that aren't exercised throw, since
// the smoke test only cares whether the surface compiles cleanly under
// IsAotCompatible=true + TrimMode=full.
// ===========================================================================

internal sealed class SmokeColumn(string name, string type): ITableColumn
{
    public string Name { get; } = name;
    public bool AllowNulls { get; set; } = true;
    public string? DefaultExpression { get; set; }
    public string Type { get; set; } = type;
    public bool IsPrimaryKey { get; set; }
}

internal sealed class SmokeIndex(string name): INamed
{
    public string Name { get; } = name;
}

internal sealed class SmokeForeignKey(string name): ForeignKeyBase(name)
{
    private string[] _columnNames = Array.Empty<string>();
    private string[] _linkedNames = Array.Empty<string>();

    public override string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value;
    }

    public override string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value;
    }

    // The Parse helper in ForeignKeyBase falls back to the supplied default
    // schema when the catalog row's table name is unqualified. The smoke
    // test passes "states" (unqualified), so we expect "smoke.states".
    public void Parse(string definition) => base.Parse(definition, defaultSchema: "smoke");

    protected override DbObjectName ParseLinkedTable(string tableName)
        => new DbObjectName(tableName.Contains('.') ? tableName.Split('.')[0] : "smoke",
                            tableName.Contains('.') ? tableName.Split('.')[1] : tableName);
}

internal sealed class SmokeTable(DbObjectName identifier)
    : TableBase<SmokeColumn, SmokeIndex, SmokeForeignKey>(identifier)
{
    public override IReadOnlyList<string> PrimaryKeyColumns
        => _columns.Where(c => c.IsPrimaryKey).Select(c => c.Name).ToList();

    protected override string DefaultPrimaryKeyName()
        => $"pk_{Identifier.Name}_{string.Join("_", PrimaryKeyColumns)}";

    protected override SmokeForeignKey CreateForeignKey(string name) => new(name);

    protected override ITableColumn AddColumnAndReturn(string name, string columnType)
    {
        var col = new SmokeColumn(name, columnType);
        _columns.Add(col);
        return col;
    }

    protected override ITableColumn AddPrimaryKeyColumnAndReturn(string name, string columnType)
    {
        var col = new SmokeColumn(name, columnType) { IsPrimaryKey = true };
        _columns.Add(col);
        return col;
    }

    protected override string GetDatabaseTypeFor(Type dotnetType) => dotnetType.Name;

    protected override Migrator GetDefaultMigratorForBasicSql() => new SmokeMigrator();

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
        => writer.Write($"CREATE TABLE {Identifier};");

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
        => writer.Write($"DROP TABLE {Identifier};");

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        // No-op — the smoke test never executes a catalog query.
    }
}

internal sealed class SmokeMigrator(): Migrator("smoke")
{
    public override IDatabaseProvider Provider
        => throw new NotSupportedException("Smoke migrator has no provider.");

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
        => throw new NotSupportedException();

    public override bool MatchesConnection(DbConnection connection) => false;

    public override ITable CreateTable(DbObjectName identifier) => new SmokeTable(identifier);

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
        => writeStep(this, writer);

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer) { }
    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer) { }
    public override string ToExecuteScriptLine(string scriptName) => string.Empty;
    public override void AssertValidIdentifier(string name) { }
    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
        => string.Empty;

    protected override Task executeDelta(SchemaMigration migration, DbConnection conn, AutoCreate autoCreate,
        IMigrationLogger logger, CancellationToken ct = default) => Task.CompletedTask;
}

internal sealed class SmokeSyntax: IDdlSyntaxStrategy
{
    public string QuoteIdentifier(string name) => $"\"{name}\"";

    public void WriteDropTable(TextWriter writer, DbObjectName identifier)
        => writer.WriteLine($"DROP TABLE IF EXISTS {identifier};");

    public void WriteCreateTableHeader(TextWriter writer, DbObjectName identifier, CreationStyle style)
    {
        if (style == CreationStyle.DropThenCreate)
        {
            writer.WriteLine($"CREATE TABLE {identifier} (");
        }
        else
        {
            writer.WriteLine($"CREATE TABLE IF NOT EXISTS {identifier} (");
        }
    }

    public bool InlineForeignKeyConstraints => false;
    public string AutoIncrementToken => "SMOKE_INCR";
    public string StatementTerminator => ";";
}

