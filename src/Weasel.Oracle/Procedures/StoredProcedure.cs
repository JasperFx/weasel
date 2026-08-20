using System.Text.RegularExpressions;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Procedures;

/// <summary>
///     An Oracle stored procedure.
/// </summary>
/// <remarks>
///     <para>
///         Oracle keeps the source verbatim, one row per line, in <c>all_source</c> — and without
///         the <c>CREATE OR REPLACE</c> prefix, which is a wrapper on the statement rather than part
///         of the object. The caller's statement is stripped of that prefix before comparing.
///     </para>
///     <para>
///         A procedure inside a package is a different object and is not this — see weasel#453.
///     </para>
/// </remarks>
public class StoredProcedure: StoredProcedureBase
{
    public StoredProcedure(string name, string body)
        : this(DbObjectName.Parse(OracleProvider.Instance, name), body)
    {
    }

    public StoredProcedure(DbObjectName identifier, string body): base(identifier, body)
    {
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // No DROP PROCEDURE IF EXISTS on Oracle, so swallow "object does not exist" the way the
        // rest of the provider does.
        writer.WriteLine($@"BEGIN
    EXECUTE IMMEDIATE 'DROP PROCEDURE {SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF;
END;");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        // LISTAGG rather than reading the rows, so this stays one result set like every other
        // schema object's query.
        builder.Append(
            "SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY line) FROM all_source "
            + $"WHERE owner = :{schemaParam} AND name = :{nameParam} AND type = 'PROCEDURE'");
    }

    /// <inheritdoc />
    protected override bool Matches(string existing)
        => Canonicize(existing)
            .Equals(Canonicize(StripCreateOrReplace(BodyText(), Identifier.Schema)),
                StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     Reduce the caller's statement to what <c>all_source</c> actually stores.
    /// </summary>
    /// <remarks>
    ///     Two things go. The <c>CREATE OR REPLACE</c> is a wrapper on the statement rather than
    ///     part of the object, so Oracle keeps the source from <c>PROCEDURE</c> onwards. And the
    ///     schema qualifier goes with it — a procedure's source names it bare, because the owner is
    ///     already the row's <c>owner</c> column. Measured, not assumed: Oracle stores
    ///     <c>PROCEDURE\t sp_stamp IS</c> for a statement that said
    ///     <c>CREATE OR REPLACE PROCEDURE WEASEL.sp_stamp IS</c>.
    /// </remarks>
    internal static string StripCreateOrReplace(string statement, string? schema = null)
    {
        var index = statement.IndexOf("PROCEDURE", StringComparison.OrdinalIgnoreCase);
        var source = (index < 0 ? statement : statement[index..]).TrimEnd().TrimEnd('/').TrimEnd();

        return schema.IsEmpty()
            ? source
            : Regex.Replace(source, $@"(?<=PROCEDURE\s+){Regex.Escape(schema!)}\s*\.\s*", "",
                RegexOptions.IgnoreCase);
    }

    /// <summary>
    ///     <c>CREATE OR REPLACE PROCEDURE</c> does the whole job, so the update is that statement
    ///     alone — see <see cref="OracleReplaceDelta" /> for why nothing may be prefixed to it.
    /// </summary>
    protected override ISchemaObjectDelta CreateDelta(string? existing)
    {
        if (IsRemoved)
        {
            // isRemoved: the update is the drop, alone -- which is a lone PL/SQL block and so a
            // perfectly good command.
            return new OracleReplaceDelta(this,
                existing == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update,
                isRemoved: true);
        }

        if (existing == null)
        {
            return new OracleReplaceDelta(this, SchemaPatchDifference.Create);
        }

        return new OracleReplaceDelta(this,
            Matches(existing) ? SchemaPatchDifference.None : SchemaPatchDifference.Update);
    }

    public async Task<string?> FetchExistingSourceAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var source = await ReadExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return source;
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => await FetchExistingSourceAsync(conn, ct).ConfigureAwait(false) != null;
}
