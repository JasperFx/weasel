using System.Data.Common;
using System.Text.RegularExpressions;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Functions;

/// <summary>
///     An Oracle stored function.
/// </summary>
/// <remarks>
///     <para>
///         Oracle keeps the source verbatim, one row per line, in <c>all_source</c> — and without
///         the <c>CREATE OR REPLACE</c> prefix, which is a wrapper on the statement rather than part
///         of the object, and without the schema qualifier, because the owner is already the row's
///         own column. Measured: a statement reading
///         <c>CREATE OR REPLACE FUNCTION WEASEL.fn_probe(n IN NUMBER) RETURN NUMBER IS</c> comes
///         back as <c>FUNCTION\tfn_probe(n IN NUMBER) RETURN NUMBER IS</c> — with a tab.
///     </para>
///     <para>
///         A function inside a package is a different object; see <c>Weasel.Oracle.Packages</c>.
///     </para>
/// </remarks>
public class Function: FunctionBase
{
    public Function(string name, string body)
        : this(DbObjectName.Parse(OracleProvider.Instance, name), body)
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
        => new(DbObjectName.Parse(OracleProvider.Instance, name), body: null) { IsRemoved = true };

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        // CREATE OR REPLACE does the whole job in one statement, which is what Oracle's migrator
        // can execute per delta.
        writer.WriteLine(RawBody!.TrimEnd().TrimEnd('/').TrimEnd());
    }

    protected override Migrator GetDefaultMigrator() => new OracleMigrator();

    protected override string[] ComputeDefaultDropStatements()
        =>
        [
            // No DROP FUNCTION IF EXISTS on Oracle, so swallow ORA-04043 the way the rest of the
            // provider does.
            $@"BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION {SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF;
END;"
        ];

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        // LISTAGG rather than reading the rows, so this stays one result set like every other
        // schema object's query.
        builder.Append(
            "SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY line) FROM all_source "
            + $"WHERE owner = :{schemaParam} AND name = :{nameParam} AND type = 'FUNCTION'");
    }

    protected override async Task<FunctionBase?> ReadExistingFromReaderAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var source = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

        return source.IsEmpty() ? null : new Function(Identifier, source);
    }

    protected override ISchemaObjectDelta CreateFunctionDelta(FunctionBase? actual)
        => new FunctionDelta(this, (Function?)actual);

    /// <summary>
    ///     Reduce the caller's statement to what <c>all_source</c> actually stores: from
    ///     <c>FUNCTION</c> onwards, with the schema qualifier removed.
    /// </summary>
    internal static string StripCreateOrReplace(string statement, string? schema = null)
    {
        var index = statement.IndexOf("FUNCTION", StringComparison.OrdinalIgnoreCase);
        var source = (index < 0 ? statement : statement[index..]).TrimEnd().TrimEnd('/').TrimEnd();

        return schema.IsEmpty()
            ? source
            : Regex.Replace(source, $@"(?<=FUNCTION\s+){Regex.Escape(schema!)}\s*\.\s*", "",
                RegexOptions.IgnoreCase);
    }

    internal string BodyForComparison() => StripCreateOrReplace(RawBody ?? string.Empty, Identifier.Schema);

    public async Task<string?> FetchExistingSourceAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var existing = await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return ((Function?)existing)?.SourceText;
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => await FetchExistingSourceAsync(conn, ct).ConfigureAwait(false) != null;

    internal string? SourceText => RawBody;
}

/// <summary>
///     Oracle applies a function change with <c>CREATE OR REPLACE FUNCTION</c> alone.
/// </summary>
/// <remarks>
///     The write behaviour is <see cref="OracleReplaceDelta" />'s, applied here rather than
///     inherited: a function delta also needs <see cref="SchemaObjectDelta{T}" />'s expected/actual
///     comparison, and a type cannot have both bases. Delegating keeps the rule in one place —
///     see that type for why a drop may not be prefixed to the create.
/// </remarks>
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
    ///     Whitespace and case. Oracle stores the source as submitted apart from the wrapper, and
    ///     puts a tab where the caller wrote a space, so whitespace has to go entirely.
    /// </summary>
    internal static string Canonicize(string sql)
        => sql.Replace("\r\n", "").Replace("\n", "").Replace("\r", "").Replace("\t", "")
            .Replace(" ", "").Trim().TrimEnd(';').ToUpperInvariant();

    /// <summary>
    ///     The drop for a removed function has to come from <see cref="SchemaObjectDelta{T}.Actual" />,
    ///     not from <c>Expected</c>.
    /// </summary>
    /// <remarks>
    ///     <see cref="FunctionBase.DropStatements" /> returns an <em>empty</em> array when
    ///     <see cref="FunctionBase.IsRemoved" /> is set — a removed function is a declaration that
    ///     something should not exist, and it carries no body to build a drop from. The object that
    ///     can produce the drop is the one read out of the database. Flattening this to
    ///     <c>Expected</c> emits nothing at all and the function survives the migration, which is
    ///     what <c>a_function_marked_for_removal_is_dropped</c> catches.
    /// </remarks>
    public override void WriteUpdate(Migrator rules, TextWriter writer)
        => OracleReplaceDelta.WriteReplacement(
            Expected.IsRemoved ? Actual! : Expected, Expected.IsRemoved, rules, writer);

    public override void WriteRollback(Migrator rules, TextWriter writer)
        => OracleReplaceDelta.WriteReplacement(Actual ?? Expected, Actual == null, rules, writer);

    public override string ToString() => $"{Expected.Identifier.QualifiedName} Diff";
}
