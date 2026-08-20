using System.Data.Common;
using JasperFx.Core;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Sqlite.Triggers;

/// <summary>
///     A SQLite trigger. SQLite stores the submitted text verbatim in <c>sqlite_master.sql</c>, so
///     the delta is a whitespace-insensitive comparison of the whole statement, the same way its
///     views work.
/// </summary>
/// <remarks>
///     <para>
///         SQLite has no <c>ALTER TRIGGER</c> and no <c>CREATE OR REPLACE</c>, so a change is a drop
///         followed by a create — and <c>WriteCreateStatement</c> emits both, because that also
///         makes creation idempotent.
///     </para>
///     <para>
///         Triggers are always row-level on SQLite: <c>FOR EACH ROW</c> is accepted but is the only
///         behaviour, and <c>FOR EACH STATEMENT</c> is a syntax error. <see cref="ForEachRow" /> is
///         therefore not emitted.
///     </para>
/// </remarks>
public class Trigger: TriggerBase
{
    public Trigger(string name, string target, string body)
        : this(
            DbObjectName.Parse(SqliteProvider.Instance, name),
            DbObjectName.Parse(SqliteProvider.Instance, target),
            body)
    {
    }

    public Trigger(DbObjectName identifier, DbObjectName target, string body): base(identifier, target, body)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);
        writer.WriteLine(CreateStatement());
    }

    /// <summary>
    ///     The single <c>CREATE TRIGGER</c> statement this trigger renders to. Public because it is
    ///     what delta comparison and diagnostics both want, the same way a view exposes
    ///     <c>ToBasicCreateViewSql</c>.
    /// </summary>
    public string CreateStatement()
    {
        var condition = Condition.IsNotEmpty() ? $" WHEN {Condition}" : string.Empty;
        var body = Body.Trim();

        if (!body.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase))
        {
            body = $"BEGIN {body.TrimEnd(';')}; END";
        }

        return
            $"CREATE TRIGGER {QualifiedName(Identifier)} {TimingKeyword()} {SingleEvent("SQLite")} "
            + $"ON {SchemaUtils.QuoteName(Target.Name)}{condition} {body};";
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TRIGGER IF EXISTS {QualifiedName(Identifier)};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append(
            $"SELECT sql FROM {SchemaUtils.QuoteName(Identifier.Schema)}.sqlite_master WHERE type = 'trigger' AND name = @{nameParam}");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readSqlAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return NormalizeBody(existing) == NormalizeBody(CreateStatement())
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<bool> ExistsInDatabaseAsync(SqliteConnection conn, CancellationToken ct = default)
        => await FetchExistingSqlAsync(conn, ct).ConfigureAwait(false) != null;

    public async Task<string?> FetchExistingSqlAsync(SqliteConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var sql = await readSqlAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return sql;
    }

    private static async Task<string?> readSqlAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false))
        {
            return null;
        }

        var sql = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(sql) ? null : sql;
    }

    private static string QualifiedName(DbObjectName name)
        => name.Schema.EqualsIgnoreCase("main")
            ? SchemaUtils.QuoteName(name.Name)
            : $"{SchemaUtils.QuoteName(name.Schema)}.{SchemaUtils.QuoteName(name.Name)}";
}
