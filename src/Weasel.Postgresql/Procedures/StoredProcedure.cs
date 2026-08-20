using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql.Procedures;

/// <summary>
///     A PostgreSQL stored procedure. Real <c>PROCEDURE</c> objects, which PostgreSQL has had since
///     v11 and which are distinct from functions: no return value, and they can manage transactions.
/// </summary>
/// <remarks>
///     <para>
///         Comparison is against <c>pg_proc.prosrc</c>, which holds the body exactly as submitted.
///         <c>pg_get_functiondef</c> is not used, because PostgreSQL renders the header rather than
///         storing it — <c>(n int, tag text)</c> comes back as <c>(IN n integer, IN tag text)</c>
///         and <c>$$</c> becomes <c>$procedure$</c> — so comparing against it reports drift on every
///         check for any procedure not written in PostgreSQL's own spelling. That is the permanent
///         drift weasel#445 and weasel#446 were about.
///     </para>
///     <para>
///         The caller's body is taken from between the outermost dollar quotes, which is
///         unambiguous by construction: that is what dollar quoting is for.
///     </para>
///     <para>
///         <strong>A changed parameter list is not a change to this procedure.</strong> PostgreSQL
///         overloads on the signature, so <c>CREATE OR REPLACE PROCEDURE</c> with different
///         parameters creates a second procedure and leaves the first one in place. Weasel reports
///         the new one as <c>Create</c>; the old one is still yours to drop.
///     </para>
/// </remarks>
public class StoredProcedure: StoredProcedureBase
{
    public StoredProcedure(string name, string body)
        : this(DbObjectName.Parse(PostgresqlProvider.Instance, name), body)
    {
    }

    public StoredProcedure(DbObjectName identifier, string body): base(identifier, body)
    {
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // No argument list: PostgreSQL accepts a bare name when it is unambiguous, and Weasel does
        // not model the signature.
        writer.WriteLine($"DROP PROCEDURE IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;

        builder.Append($@"
SELECT p.prosrc
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.prokind = 'p' AND p.proname = :{nameParam} AND n.nspname = :{schemaParam}");
    }

    /// <inheritdoc />
    protected override bool Matches(string existing)
        => Canonicize(existing).Equals(Canonicize(ExtractBody(BodyText())), StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     The contents between the outermost dollar quotes — <c>$$ … $$</c> or a tagged
    ///     <c>$name$ … $name$</c> — which is what <c>pg_proc.prosrc</c> holds.
    /// </summary>
    internal static string ExtractBody(string statement)
    {
        var open = statement.IndexOf('$');
        if (open < 0) return statement;

        var tagEnd = statement.IndexOf('$', open + 1);
        if (tagEnd < 0) return statement;

        var tag = statement.Substring(open, tagEnd - open + 1);
        var bodyStart = tagEnd + 1;
        var close = statement.IndexOf(tag, bodyStart, StringComparison.Ordinal);

        return close < 0 ? statement[bodyStart..] : statement[bodyStart..close];
    }

    public async Task<string?> FetchExistingBodyAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await ReadExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body;
    }

    public async Task<bool> ExistsInDatabaseAsync(NpgsqlConnection conn, CancellationToken ct = default)
        => await FetchExistingBodyAsync(conn, ct).ConfigureAwait(false) != null;
}
