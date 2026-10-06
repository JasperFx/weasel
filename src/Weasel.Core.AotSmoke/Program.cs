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
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx;
using JasperFx.Core.Reflection;
using Weasel.Core;
using Weasel.Core.Identity;
using Weasel.Core.Migrations;
using Weasel.Core.Sequences;
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

// --- Identifications.ForValueType ---------------------------------------
// weasel#690. A strong-typed id is the one identity shape whose strategy
// cannot be built by closing a generic under AOT: TInner is the wrapped
// primitive and TOuter is usually a readonly record struct, so two of the
// three type arguments are value types and the instantiation has no native
// code. Identifications.ForValueType branches on
// RuntimeFeature.IsDynamicCodeSupported, which ILC substitutes to false and
// then trims — so this call site must produce NO IL3050, even though the
// branch it does not take calls MakeGenericType. That is what this section
// holds: IL3050 is an error in this project.
//
// IL2026 is suppressed rather than avoided, because it is accurate and not
// what is under test here: the strategy really does read the id member and
// the wrapper's value property reflectively, and a consumer keeps both
// rooted. The AOT dimension is the one weasel#690 is about.

if (!IdentitySmoke.StrongTypedIdRoundTrips())
{
    Console.Error.WriteLine("Identifications.ForValueType regression.");
    return 1;
}

// --- The other six Identifications factories ----------------------------
// weasel#694. These are the shapes that look safe and are not. Each closes a
// generic over the document type alone, and a document type is a class, so
// MakeGenericType has a canonical body to share -- IF ILC generated one.
// Nothing statically references SequentialGuidIdentification<anything>, so
// without a [DynamicDependency] rooting the strategy there is no canonical
// instantiation to share and MakeGenericType throws:
//
//   NotSupportedException: 'Weasel.Core.Identity.SequentialGuidIdentification`1[Doc]'
//     is missing native code or metadata.
//
// A consumer that happens to construct the same strategy itself roots it by
// accident, which is why this arrived from Polecat as the SECOND failure --
// MissingMethodException from Activator, the canonical body present and only
// the ctor metadata trimmed. Both layers are the same missing root.
//
// None of that is reachable by the analyzer: it is a runtime property of the
// native image, so only a native publish of this project proves it. Build
// warnings are necessary and not sufficient here -- see CLAUDE.md.
if (!IdentitySmoke.EveryFactoryConstructsAndRuns(out var failure))
{
    Console.Error.WriteLine($"Identifications regression (weasel#694): {failure}");
    return 1;
}

Console.WriteLine($"Weasel.Core AOT smoke OK — exercised {nameof(DbObjectName)}, " +
                  $"{nameof(ForeignKeyBase)}.Parse, {nameof(TableBase<SmokeColumn, SmokeIndex, SmokeForeignKey>)}, " +
                  $"{nameof(IDdlSyntaxStrategy)}, every {nameof(Identifications)} factory.");
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
    public bool IsAutoNumber { get; set; }
    public string? ComputedExpression { get; set; }
    public bool ComputedColumnIsStored { get; set; }
}

internal sealed class SmokeIndex(string name): ITableIndex
{
    public string Name { get; } = name;
    public string[] Columns { get; set; } = Array.Empty<string>();
    public bool IsUnique { get; set; }
    public string? Predicate { get; set; }
    public string[]? IncludeColumns { get; set; }
    public string? Method { get; set; }
    public bool HasProviderSpecificOptions => false;

    public string ToDDL(ITable parent)
        => $"CREATE INDEX {Name} ON {parent.Identifier} ({string.Join(", ", Columns)});";
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

    protected override SmokeIndex CreateIndexFor(string name, string[] columnNames)
        => new(name) { Columns = columnNames };

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



/// <summary>
///     weasel#690's shape: a document whose id is a wrapper struct over a Guid.
/// </summary>
internal readonly record struct SmokeId(Guid Value);

internal sealed class SmokeDocument
{
    public SmokeId Id { get; set; }
}

internal static class IdentitySmoke
{
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification =
            "The strong-typed id strategy reads the id member and the wrapper's value property reflectively by design; both are rooted by this project. The AOT dimension (IL3050) is what this smoke test holds, and it is not suppressed.")]
    public static bool StrongTypedIdRoundTrips()
    {
        var idMember = typeof(SmokeDocument).GetProperty(nameof(SmokeDocument.Id))!;
        var valueType = ValueTypeInfo.ForType(typeof(SmokeId));

        var identification = Identifications.ForValueType(typeof(SmokeDocument), idMember, valueType,
            typeof(SmokeDocument));

        var document = new SmokeDocument();
        var assigned = identification.AssignIfMissing(document, new SmokeSequenceSource());

        return assigned is SmokeId { Value: var value }
               && value != Guid.Empty
               && document.Id.Value == value
               && identification.AssignIfMissing(document, new SmokeSequenceSource()).Equals(assigned)
               && identification.RawSqlType == typeof(Guid)
               && identification.ToRawSqlValue(assigned).Equals(value);
    }

    /// <summary>
    ///     weasel#694. Constructs every remaining strategy through its factory and then uses it, because
    ///     construction succeeding is not the same as working: the ctors build FEC-compiled accessor
    ///     delegates, which is the other thing a native image does not have.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification =
            "Each strategy reads its document's id member reflectively by design, and this project roots every document type it passes. The AOT dimension (IL3050) is what this smoke test holds.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification =
            "weasel#694: the strategies are rooted by [DynamicDependency] on the factories, so the MakeGenericType inside Close has a canonical instantiation to share. The analyzer cannot see that, and the native run below is what proves it.")]
    public static bool EveryFactoryConstructsAndRuns(out string failure)
    {
        var guidMember = typeof(GuidIdDocument).GetProperty(nameof(GuidIdDocument.Id))!;
        var intMember = typeof(IntIdDocument).GetProperty(nameof(IntIdDocument.Id))!;
        var longMember = typeof(LongIdDocument).GetProperty(nameof(LongIdDocument.Id))!;
        var stringMember = typeof(StringIdDocument).GetProperty(nameof(StringIdDocument.Id))!;

        var cases = new (string Name, Func<IIdentification> Build, Func<object> Document, object Expected)[]
        {
            (nameof(Identifications.ForSequentialGuid),
                () => Identifications.ForSequentialGuid(typeof(GuidIdDocument), guidMember),
                () => new GuidIdDocument(), null!),
            (nameof(Identifications.ForRandomGuid),
                () => Identifications.ForRandomGuid(typeof(GuidIdDocument), guidMember),
                () => new GuidIdDocument(), null!),
            (nameof(Identifications.ForHiloInt),
                () => Identifications.ForHiloInt(typeof(IntIdDocument), intMember, typeof(IntIdDocument)),
                () => new IntIdDocument(), 42),
            (nameof(Identifications.ForHiloLong),
                () => Identifications.ForHiloLong(typeof(LongIdDocument), longMember, typeof(LongIdDocument)),
                () => new LongIdDocument(), 42L),
            (nameof(Identifications.ForIdentityKey),
                () => Identifications.ForIdentityKey(typeof(StringIdDocument), stringMember, "docs",
                    typeof(StringIdDocument)),
                () => new StringIdDocument(), "docs/42"),
            (nameof(Identifications.ForExternallyAssignedString),
                () => Identifications.ForExternallyAssignedString(typeof(StringIdDocument), stringMember),
                () => new StringIdDocument { Id = "assigned-outside" }, "assigned-outside")
        };

        foreach (var (name, build, document, expected) in cases)
        {
            IIdentification identification;
            try
            {
                identification = build();
            }
            catch (Exception e)
            {
                failure = $"{name} could not build its strategy: {e.GetType().Name}: {e.Message}";
                return false;
            }

            var target = document();
            var assigned = identification.AssignIfMissing(target, new FixedSequenceSource());

            // A Guid strategy generates its own value, so only assert that it produced one and that the
            // strategy reads back what it wrote. The rest have a known answer.
            if (expected is null)
            {
                if (assigned is not Guid { } guid || guid == Guid.Empty)
                {
                    failure = $"{name} assigned {assigned ?? "null"} rather than a generated Guid";
                    return false;
                }
            }
            else if (!expected.Equals(assigned))
            {
                failure = $"{name} assigned {assigned ?? "null"} rather than {expected}";
                return false;
            }

            if (!Equals(identification.Identity(target), assigned))
            {
                failure = $"{name} read back {identification.Identity(target)} after assigning {assigned}";
                return false;
            }
        }

        failure = string.Empty;
        return true;
    }
}

internal sealed class SmokeSequenceSource: ISequenceSource
{
    public ISequence SequenceFor(Type documentType)
        => throw new NotSupportedException("The smoke document's id is a Guid, so no sequence is needed.");
}

/// <summary>
///     weasel#694's shapes: documents whose id is a plain primitive, so the strategy closes a generic
///     over the document type alone.
/// </summary>
internal sealed class GuidIdDocument
{
    public Guid Id { get; set; }
}

internal sealed class IntIdDocument
{
    public int Id { get; set; }
}

internal sealed class LongIdDocument
{
    public long Id { get; set; }
}

internal sealed class StringIdDocument
{
    public string Id { get; set; } = string.Empty;
}

/// <summary>
///     A sequence that hands back a fixed value, so the Hi-Lo and identity-key strategies can be run
///     without a database.
/// </summary>
internal sealed class FixedSequenceSource: ISequenceSource
{
    public ISequence SequenceFor(Type documentType) => new FixedSequence();

    private sealed class FixedSequence: ISequence
    {
        public int MaxLo => 1;
        public int NextInt() => 42;
        public long NextLong() => 42L;
        public Task SetFloor(long floor) => Task.CompletedTask;
    }
}
