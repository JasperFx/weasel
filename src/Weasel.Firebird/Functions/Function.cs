using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using JasperFx.Core;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird.Functions;

/// <summary>
///     A Firebird PSQL function (Firebird 3 and later), given as its whole statement:
///     <c>CREATE FUNCTION name (…) RETURNS type AS BEGIN … END</c>.
/// </summary>
/// <remarks>
///     <para>
///         Firebird keeps the body as source and records the rest of the header in
///         <c>RDB$FUNCTION_ARGUMENTS</c>, so a delta compares the whole definition: every parameter's
///         name, type, <c>NOT NULL</c>, collation and default, the return type, <c>DETERMINISTIC</c>,
///         <c>SQL SECURITY</c> (Firebird 4 and later), and the body. A type written with a synonym --
///         <c>INT</c> for <c>INTEGER</c> -- matches the type it is stored as, and a character set,
///         collation or precision the statement leaves out is not compared.
///     </para>
///     <para>
///         Whatever verb the statement is written with, it is run as <c>CREATE OR ALTER FUNCTION</c>,
///         which creates the function or changes it in place, callers and grants intact.
///     </para>
///     <para>
///         A function inside a package, and an external (UDR) function, are different objects and are
///         not this.
///     </para>
/// </remarks>
public class Function: FunctionBase
{
    private PsqlRoutine? _parsed;

    public Function(string name, string body)
        : this(FirebirdProvider.Instance.Parse(name), body)
    {
    }

    public Function(DbObjectName identifier, string? body)
        : base(FirebirdObjectName.From(identifier ?? throw new ArgumentNullException(nameof(identifier))), body)
    {
    }

    public Function(DbObjectName identifier, string body, string[] dropStatements)
        : base(FirebirdObjectName.From(identifier ?? throw new ArgumentNullException(nameof(identifier))), body,
            dropStatements)
    {
    }

    /// <summary>
    ///     Mark this function for removal: the migration drops it and creates nothing.
    /// </summary>
    public static Function ForRemoval(string name)
        => new(FirebirdProvider.Instance.Parse(name), body: null) { IsRemoved = true };

    /// <summary>
    ///     The function's name as Firebird's catalog stores it.
    /// </summary>
    internal string CatalogName => SchemaUtils.CatalogName(Identifier.Name);

    /// <summary>
    ///     The routine as read from the catalog, when this function was.
    /// </summary>
    internal PsqlRoutine? Catalog { get; private init; }

    /// <summary>
    ///     The statement, parsed. Refuses a statement that is not a PSQL function, or that names a
    ///     different function from this one's identifier -- a mismatch that would otherwise create one
    ///     function and look for another, and report it missing forever.
    /// </summary>
    internal PsqlRoutine Parsed => _parsed ??= parse(null);

    private PsqlRoutine parse(FirebirdServerVersion? version)
    {
        if (RawBody.IsEmpty())
        {
            throw new InvalidOperationException($"Function {Identifier} has no statement to create it from.");
        }

        var routine = PsqlRoutine.Parse(RawBody!, PsqlRoutineKind.Function, version);
        assertNamesThisFunction(routine);

        return routine;
    }

    private void assertNamesThisFunction(PsqlRoutine routine)
    {
        if (routine.CatalogName != CatalogName)
        {
            throw new ArgumentException(
                $"Function {Identifier.Name} is stored as {CatalogName}, but its statement creates {routine.CatalogName}. "
                + "Firebird folds an undelimited name to upper case and keeps a delimited one as written, so name "
                + "the function the same way in both places.");
        }
    }

    /// <summary>
    ///     <c>CREATE OR ALTER FUNCTION</c>, as a PSQL statement.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        FirebirdObjectName.AssertDefaultSchema(Identifier.Schema, $"function {Identifier.Name}");
        FirebirdScript.WritePsql(writer, Parsed.Statement);
    }

    /// <summary>
    ///     Each drop statement as its own statement: a PSQL block through <see cref="FirebirdScript.WritePsql" />,
    ///     anything else through <see cref="FirebirdScript.WriteStatement" />, so it splits back out.
    /// </summary>
    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        foreach (var statement in DropStatements())
        {
            if (statement.TrimStart().StartsWith("EXECUTE BLOCK", StringComparison.OrdinalIgnoreCase))
            {
                FirebirdScript.WritePsql(writer, statement);
            }
            else
            {
                FirebirdScript.WriteStatement(writer, statement);
            }
        }
    }

    protected override Migrator GetDefaultMigrator() => new FirebirdMigrator();

    /// <summary>
    ///     A <c>DROP FUNCTION</c> that runs only while the function exists. Firebird before 6 has no
    ///     <c>DROP FUNCTION IF EXISTS</c>.
    /// </summary>
    protected override string[] ComputeDefaultDropStatements()
        => [FirebirdScript.Guarded(CatalogProbes.Function(CatalogName), $"DROP FUNCTION {SchemaUtils.QuoteName(Identifier.Name)}", whenExists: true)];

    /// <summary>
    ///     Firebird executes one statement per command, so introspection goes through
    ///     <see cref="FirebirdDbCommandBuilder" />.
    /// </summary>
    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    /// <summary>
    ///     One query: a row per argument, the return value included, each carrying the function's source
    ///     and options. Unterminated, because Firebird runs one statement per command.
    /// </summary>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var name = builder.AddParameter(CatalogName).ParameterName;
        builder.Append(PsqlRoutine.FunctionQuery("@" + name, PsqlRoutine.ReadsSqlSecurity(builder)));
    }

    protected override async Task<FunctionBase?> ReadExistingFromReaderAsync(DbDataReader reader, CancellationToken ct)
    {
        var routine = await PsqlRoutine.ReadAsync(reader, PsqlRoutineKind.Function, CatalogName, ct).ConfigureAwait(false);

        return routine == null ? null : new Function(Identifier, routine.Statement) { Catalog = routine };
    }

    protected override ISchemaObjectDelta CreateFunctionDelta(FunctionBase? actual)
    {
        var existing = (Function?)actual;

        if (IsRemoved)
        {
            return new CreateOrAlterDelta(this, existing,
                existing == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update, isRemoved: true);
        }

        if (existing?.Catalog == null)
        {
            return new CreateOrAlterDelta(this, null, SchemaPatchDifference.Create);
        }

        var differences = parse(existing.Catalog.Version).DifferencesFrom(existing.Catalog);

        return new CreateOrAlterDelta(this, existing,
            differences.Count == 0 ? SchemaPatchDifference.None : SchemaPatchDifference.Update,
            differences: differences);
    }

    /// <summary>
    ///     The function as the database has it, rebuilt into a statement that would create it, or null
    ///     when there is no function of this name.
    /// </summary>
    public async Task<Function?> FetchExistingAsync(FbConnection conn, CancellationToken ct = default)
    {
        var builder = new FirebirdDbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        return (Function?)await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
    }

    public async Task<bool> ExistsInDatabaseAsync(FbConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;

    /// <summary>
    ///     The statement this function was given, or rebuilt from the catalog.
    /// </summary>
    public string? Statement => RawBody;
}
