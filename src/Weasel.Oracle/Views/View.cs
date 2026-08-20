using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Views;

/// <summary>
///     An Oracle view. Unlike SQL Server and MySQL, Oracle stores the view text exactly as it was
///     submitted — <c>all_views.TEXT</c> hands back the caller's own SELECT — so delta detection is
///     a whitespace-insensitive comparison of the body, the same as SQL Server and SQLite.
/// </summary>
/// <remarks>
///     <c>CREATE OR REPLACE VIEW</c> rather than drop-then-create: it is idempotent, it is supported
///     on every Oracle version Weasel targets, and it avoids <c>DROP VIEW IF EXISTS</c>, which only
///     arrived in 23c.
/// </remarks>
public class View: ViewBase
{
    public View(string viewName, string viewSql)
        : this(
            viewName != null
                ? DbObjectName.Parse(OracleProvider.Instance, viewName)
                : throw new ArgumentNullException(nameof(viewName)),
            viewSql)
    {
    }

    public View(DbObjectName identifier, string viewSql): base(identifier, viewSql)
    {
    }

    /// <inheritdoc />
    protected override DbObjectName WithSchema(string schemaName)
        => new OracleObjectName(schemaName, Identifier.Name);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new OracleMigrator { Formatting = SqlFormatting.Concise };

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var body = ViewSql.TrimEnd().TrimEnd(';');

        writer.WriteLine($"CREATE OR REPLACE VIEW {Identifier.QualifiedName} AS {body}");
    }

    public override void WriteDropStatement(Migrator migrator, TextWriter writer)
    {
        // No DROP VIEW IF EXISTS before 23c, so swallow "view does not exist" the way the rest of
        // the Oracle provider does.
        writer.WriteLine($@"BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW {SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;");
    }

    /// <summary>
    ///     <c>ALL_VIEWS.TEXT</c> is a LONG column, and ODP.NET's <c>InitialLONGFetchSize</c>
    ///     defaults to 0 — the column reads back as an empty string, so a view that plainly exists
    ///     looks like it does not. <c>-1</c> fetches the whole value however long it is.
    /// </summary>
    /// <remarks>
    ///     The two alternatives both lose. <c>ALL_VIEWS.TEXT_VC</c> is a VARCHAR2 mirror of the
    ///     same column, but it is 4000 characters and 18c+, so a longer view silently truncates and
    ///     reports drift forever — the disease weasel#445 and weasel#446 were about.
    ///     <c>dbms_metadata.get_ddl</c> returns a CLOB and never truncates, but it hands back the
    ///     whole <c>CREATE OR REPLACE FORCE NONEDITIONABLE VIEW … AS</c> header, and <c>AS</c>
    ///     occurs inside view bodies, so extracting the body is guesswork.
    /// </remarks>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        if (builder.Command is OracleCommand oracleCommand)
        {
            oracleCommand.InitialLONGFetchSize = -1;
        }

        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append(
            $"SELECT text FROM all_views WHERE owner = :{schemaParam} AND view_name = :{nameParam}");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readBodyAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new OracleReplaceDelta(this, SchemaPatchDifference.Create);
        }

        return NormalizeSql(existing) == NormalizeSql(ViewSql)
            ? new OracleReplaceDelta(this, SchemaPatchDifference.None)
            : new OracleReplaceDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<View?> FetchExistingAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await readBodyAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body == null ? null : new View(Identifier, body);
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readBodyAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        var text = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

        return string.IsNullOrWhiteSpace(text) ? null : text.Trim();
    }

    internal static string NormalizeSql(string sql)
        => sql.Replace("\r\n", "")
            .Replace("\n", "")
            .Replace("\r", "")
            .Replace("\t", "")
            .Replace(" ", "")
            .Trim()
            .TrimEnd(';')
            .ToUpperInvariant();
}
