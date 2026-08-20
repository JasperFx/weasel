using System.Data.Common;
using JasperFx.Core;
using MySqlConnector;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.MySql.Functions;

/// <summary>
///     A MySQL stored function.
/// </summary>
/// <remarks>
///     <para>
///         MySQL keeps a routine's body verbatim in
///         <c>information_schema.ROUTINES.ROUTINE_DEFINITION</c> — unlike a view definition, which
///         it rewrites — so comparison is a canonicalized match on the body. What it stores is the
///         body alone, from <c>BEGIN</c> onwards, without the <c>CREATE FUNCTION …</c> header and
///         without the <c>RETURNS</c> clause, so the caller's statement is trimmed to match.
///     </para>
///     <para>
///         There is no <c>CREATE OR REPLACE FUNCTION</c>, so a change is a drop followed by a
///         create. Both go to the server in one command; MySqlConnector executes them in order,
///         which is why weasel#452 stopped the migrator splitting delta SQL on semicolons — a
///         function body is full of them.
///     </para>
///     <para>
///         Creating one needs <c>CREATE ROUTINE</c>, and on a server with binary logging enabled it
///         also needs <c>SUPER</c> or <c>log_bin_trust_function_creators</c>. MySQL refuses
///         otherwise with a message about the SUPER privilege that never mentions functions.
///     </para>
/// </remarks>
public class Function: FunctionBase
{
    public Function(string name, string body)
        : this(DbObjectName.Parse(MySqlProvider.Instance, name), body)
    {
    }

    public Function(DbObjectName identifier, string? body): base(identifier, body)
    {
    }

    public Function(DbObjectName identifier, string body, string[] dropStatements)
        : base(identifier, body, dropStatements)
    {
    }

    /// <summary>
    ///     Mark this function for removal: the migration drops it and creates nothing.
    /// </summary>
    public static Function ForRemoval(string name)
        => new(DbObjectName.Parse(MySqlProvider.Instance, name), body: null) { IsRemoved = true };

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        WriteDropStatement(migrator, writer);
        writer.WriteLine(RawBody!.TrimEnd().TrimEnd(';'));
    }

    protected override Migrator GetDefaultMigrator() => new MySqlMigrator();

    protected override string[] ComputeDefaultDropStatements()
        => [$"DROP FUNCTION IF EXISTS {QualifiedName()};"];

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append(
            "SELECT routine_definition FROM information_schema.ROUTINES "
            + $"WHERE routine_type = 'FUNCTION' AND routine_schema = @{schemaParam} AND routine_name = @{nameParam}");
    }

    protected override async Task<FunctionBase?> ReadExistingFromReaderAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var body = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

        return body.IsEmpty() ? null : new Function(Identifier, body);
    }

    protected override ISchemaObjectDelta CreateFunctionDelta(FunctionBase? actual)
        => new FunctionDelta(this, (Function?)actual);

    /// <summary>
    ///     The body as <c>ROUTINE_DEFINITION</c> stores it: everything from the first
    ///     <c>BEGIN</c>. A function read back out of the catalog is already in that form, so this is
    ///     idempotent and both sides of a comparison can go through it.
    /// </summary>
    internal static string ExtractBody(string statement)
    {
        var index = statement.IndexOf("BEGIN", StringComparison.OrdinalIgnoreCase);
        return (index < 0 ? statement : statement[index..]).TrimEnd().TrimEnd(';');
    }

    internal string BodyForComparison() => ExtractBody(RawBody ?? string.Empty);

    private string QualifiedName()
        => $"{SchemaUtils.QuoteName(Identifier.Schema)}.{SchemaUtils.QuoteName(Identifier.Name)}";

    public async Task<string?> FetchExistingBodyAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var existing = await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return ((Function?)existing)?.RawBody;
    }

    public async Task<bool> ExistsInDatabaseAsync(MySqlConnection conn, CancellationToken ct = default)
        => await FetchExistingBodyAsync(conn, ct).ConfigureAwait(false) != null;

    internal new string? RawBody => base.RawBody;
}

/// <summary>
///     MySQL has no <c>CREATE OR REPLACE FUNCTION</c>, so an update is a drop and a create — which
///     is exactly what <see cref="Function.WriteCreateStatement" /> already emits.
/// </summary>
public class FunctionDelta: SchemaObjectDelta<Function>
{
    public FunctionDelta(Function expected, Function? actual): base(expected, actual)
    {
    }

    protected override SchemaPatchDifference compare(Function expected, Function? actual)
    {
        if (expected.IsRemoved)
        {
            return actual == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update;
        }

        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        return Canonicize(expected.BodyForComparison()) == Canonicize(actual.BodyForComparison())
            ? SchemaPatchDifference.None
            : SchemaPatchDifference.Update;
    }

    /// <summary>
    ///     Whitespace and case only. MySQL stores the body as submitted, so there is nothing else to
    ///     absorb — unlike a view, whose definition it rewrites entirely.
    /// </summary>
    internal static string Canonicize(string sql)
        => sql.Replace("\r\n", "").Replace("\n", "").Replace("\r", "").Replace("\t", "")
            .Replace(" ", "").Trim().TrimEnd(';').ToUpperInvariant();

    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Expected.IsRemoved)
        {
            Actual!.WriteDropStatement(rules, writer);
            return;
        }

        Expected.WriteCreateStatement(rules, writer);
    }

    public override void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Actual == null)
        {
            Expected.WriteDropStatement(rules, writer);
            return;
        }

        Actual.WriteCreateStatement(rules, writer);
    }

    public override string ToString() => $"{Expected.Identifier.QualifiedName} Diff";
}
