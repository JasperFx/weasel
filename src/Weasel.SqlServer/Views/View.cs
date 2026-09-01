using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Views;

/// <summary>
///     A SQL Server view. Changes are applied by dropping and recreating, because
///     <c>ALTER VIEW</c> replaces the whole definition anyway and the drop keeps the path
///     identical whether or not the view is already there.
/// </summary>
/// <remarks>
///     SQL Server requires <c>CREATE VIEW</c> to be the only statement in its batch, so the
///     create goes inside <c>EXEC sp_executesql</c> — the same reason
///     <see cref="Functions.Function" /> does it. That puts the body inside a string literal,
///     so its own single quotes have to be doubled.
/// </remarks>
public class View: ViewBase
{
    public View(string viewName, string viewSql)
        : this(
            viewName != null
                ? DbObjectName.Parse(SqlServerProvider.Instance, viewName)
                : throw new ArgumentNullException(nameof(viewName)),
            viewSql)
    {
    }

    public View(DbObjectName identifier, string viewSql): base(identifier, viewSql)
    {
    }

    /// <inheritdoc />
    protected override DbObjectName WithSchema(string schemaName)
        => new SqlServerObjectName(schemaName, Identifier.Name);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new SqlServerMigrator { Formatting = SqlFormatting.Concise };

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);

        var body = ViewSql.TrimEnd().TrimEnd(';');
        var create = $"CREATE VIEW {Identifier} AS {body}";

        writer.WriteLine($"EXEC sp_executesql N'{SchemaUtils.EscapeLiteral(create)}';");
    }

    public override void WriteDropStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"DROP VIEW IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.ToString()).ParameterName;
        builder.Append(
            $"SELECT sm.definition FROM sys.sql_modules AS sm INNER JOIN sys.views AS v ON v.object_id = sm.object_id WHERE sm.object_id = OBJECT_ID(@{nameParam});");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readBodyAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return NormalizeSql(existing) == NormalizeSql(ViewSql)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<View?> FetchExistingAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await readBodyAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body == null ? null : new View(Identifier, body);
    }

    public static Task<View?> FetchExistingAsync(SqlConnection conn, DbObjectName identifier,
        CancellationToken ct = default)
        => new View(identifier, "select 1 as one").FetchExistingAsync(conn, ct);

    public async Task<bool> ExistsInDatabaseAsync(SqlConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readBodyAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        var definition = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

        return string.IsNullOrEmpty(definition) ? null : ViewDefinition.ExtractBody(definition, SqlServerIdentifierDelimiters);
    }

    /// <summary>
    ///     SQL Server delimits an identifier with <c>[...]</c>, and with <c>"..."</c> under
    ///     <c>QUOTED_IDENTIFIER ON</c>.
    /// </summary>
    private const string SqlServerIdentifierDelimiters = "[\"";

    /// <summary>
    ///     Whitespace-insensitive and case-insensitive, because <c>sys.sql_modules</c> hands back
    ///     the text as it was submitted and callers reformat freely.
    /// </summary>
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
