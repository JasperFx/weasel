using System.Data.Common;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Packages;

/// <summary>
///     An Oracle package: a specification declaring what the package exposes, and a body
///     implementing it.
/// </summary>
/// <remarks>
///     <para>
///         This is why packages did not fit <see cref="FunctionBase" />'s single-body shape
///         (weasel#453). The spec and the body are <em>two objects</em> — <c>all_source</c> lists
///         them as <c>PACKAGE</c> and <c>PACKAGE BODY</c>, they compile separately, and each has its
///         own validity. A body can be invalid while the spec is fine, which is the normal state
///         after a base table changes, and callers of the package see that as ORA-04063 rather than
///         as a missing object.
///     </para>
///     <para>
///         So <see cref="Body" /> is optional. A spec-only package is legal Oracle — it is how you
///         declare shared constants and types — and Weasel models it rather than insisting on
///         something to implement.
///     </para>
///     <para>
///         Dropping the spec drops the body with it, so there is only one drop statement.
///     </para>
/// </remarks>
public class Package: SchemaObjectBase
{
    public Package(string name, string specification, string? body = null)
        : this(DbObjectName.Parse(OracleProvider.Instance, name), specification, body)
    {
    }

    public Package(DbObjectName identifier, string specification, string? body = null): base(identifier)
    {
        Specification = specification ?? throw new ArgumentNullException(nameof(specification));
        Body = body;
    }

    /// <summary>The <c>CREATE OR REPLACE PACKAGE …</c> statement.</summary>
    public string Specification { get; }

    /// <summary>
    ///     The <c>CREATE OR REPLACE PACKAGE BODY …</c> statement, or null for a spec-only package.
    /// </summary>
    public string? Body { get; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine(Specification.TrimEnd().TrimEnd('/').TrimEnd());

        if (Body.IsNotEmpty())
        {
            // The separator Oracle's migrator splits on: a package spec and a package body are two
            // statements, and ODP.NET executes one at a time.
            writer.WriteLine("/");
            writer.WriteLine(Body!.TrimEnd().TrimEnd('/').TrimEnd());
        }
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // Dropping the spec takes the body with it. ORA-04043: object does not exist.
        writer.WriteLine($@"BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE {SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF;
END;");
    }

    /// <summary>
    ///     Two result sets, spec then body — which Oracle can do now that a schema object may
    ///     register more than one statement (weasel#474).
    /// </summary>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append(
            "SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY line) FROM all_source "
            + $"WHERE owner = :{schemaParam} AND name = :{nameParam} AND type = 'PACKAGE'");

        builder.StartNewCommand();

        builder.Append(
            "SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY line) FROM all_source "
            + $"WHERE owner = :{schemaParam} AND name = :{nameParam} AND type = 'PACKAGE BODY'");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existingSpec = await readSourceAsync(reader, ct).ConfigureAwait(false);

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        var existingBody = await readSourceAsync(reader, ct).ConfigureAwait(false);

        if (existingSpec == null)
        {
            return new PackageDelta(this, SchemaPatchDifference.Create);
        }

        var specMatches = Matches(existingSpec, Specification);
        var bodyMatches = Body.IsEmpty()
            ? existingBody == null
            : existingBody != null && Matches(existingBody, Body!);

        return specMatches && bodyMatches
            ? new PackageDelta(this, SchemaPatchDifference.None)
            : new PackageDelta(this, SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     <c>all_source</c> stores neither the <c>CREATE OR REPLACE</c> wrapper nor the schema
    ///     qualifier — the owner is already the row's own column — exactly as with a standalone
    ///     procedure.
    /// </summary>
    internal bool Matches(string existing, string expected)
        => StoredProcedureBase.Canonicize(existing)
            .Equals(StoredProcedureBase.Canonicize(Strip(expected, Identifier.Schema)),
                StringComparison.OrdinalIgnoreCase);

    internal static string Strip(string statement, string schema)
    {
        var index = statement.IndexOf("PACKAGE", StringComparison.OrdinalIgnoreCase);
        var source = (index < 0 ? statement : statement[index..]).TrimEnd().TrimEnd('/').TrimEnd();

        return System.Text.RegularExpressions.Regex.Replace(
            source,
            $@"(?<=PACKAGE\s)(\s*BODY\s+)?{System.Text.RegularExpressions.Regex.Escape(schema)}\s*\.\s*",
            m => m.Groups[1].Success ? m.Groups[1].Value : "",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    /// <inheritdoc />
    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn) => new OracleDbCommandBuilder();

    public async Task<(string? Specification, string? Body)> FetchExistingAsync(
        OracleConnection conn, CancellationToken ct = default)
    {
        // The Oracle builder, not the neutral one: this registers two statements, and ODP.NET
        // executes one per command. OracleDbCommandBuilder splits on the boundary and the reader
        // chains across the pieces (weasel#474).
        var builder = new OracleDbCommandBuilder();
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var spec = await readSourceAsync(reader, ct).ConfigureAwait(false);
        await reader.NextResultAsync(ct).ConfigureAwait(false);
        var body = await readSourceAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return (spec, body);
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => (await FetchExistingAsync(conn, ct).ConfigureAwait(false)).Specification != null;

    private static async Task<string?> readSourceAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var source = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return source.IsEmpty() ? null : source;
    }
}

/// <summary>
///     A package applies as <c>CREATE OR REPLACE PACKAGE</c> and, when there is one,
///     <c>CREATE OR REPLACE PACKAGE BODY</c> — two statements, separated by the <c>/</c> Oracle's
///     migrator splits on.
/// </summary>
internal class PackageDelta: ISchemaObjectDelta
{
    private readonly Package _package;

    public PackageDelta(Package package, SchemaPatchDifference difference)
    {
        _package = package;
        Difference = difference;
    }

    public ISchemaObject SchemaObject => _package;

    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer) => _package.WriteCreateStatement(rules, writer);

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
        => throw new NotSupportedException();
}
