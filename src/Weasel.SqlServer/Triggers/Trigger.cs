using System.Data.Common;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Triggers;

/// <summary>
///     A SQL Server trigger.
/// </summary>
/// <remarks>
///     <para>
///         SQL Server triggers are statement-level only — there is no <c>FOR EACH ROW</c>, and a
///         trigger sees the affected rows through the <c>inserted</c> and <c>deleted</c>
///         pseudo-tables instead. <see cref="TriggerBase.ForEachRow" /> is therefore not emitted,
///         and a <c>WHEN</c> condition is rejected rather than dropped, since the engine has no
///         equivalent.
///     </para>
///     <para>
///         <c>CREATE TRIGGER</c> has to be the first statement in its batch, so the create goes
///         inside <c>EXEC sp_executesql</c> — the same reason <c>Function</c> and <c>View</c> do it.
///     </para>
/// </remarks>
public class Trigger: TriggerBase
{
    public Trigger(string name, string target, string body)
        : this(
            DbObjectName.Parse(SqlServerProvider.Instance, name),
            DbObjectName.Parse(SqlServerProvider.Instance, target),
            body)
    {
    }

    public Trigger(DbObjectName identifier, DbObjectName target, string body): base(identifier, target, body)
    {
    }

    public bool? IsDisabled { get; set; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);

        writer.WriteLine($"EXEC sp_executesql N'{SchemaUtils.EscapeLiteral(CreateStatement())}';");

        if (IsDisabled == true)
        {
            writer.WriteLine($"DISABLE TRIGGER {Identifier} ON {Target};");
        }
    }

    /// <summary>
    ///     The single <c>CREATE TRIGGER</c> statement this trigger renders to. Public because it is
    ///     what delta comparison and diagnostics both want, the same way a view exposes
    ///     <c>ToBasicCreateViewSql</c>.
    /// </summary>
    public string CreateStatement()
    {
        if (Condition.IsNotEmpty())
        {
            throw new NotSupportedException(
                $"SQL Server has no WHEN clause on a trigger, but trigger {Identifier} declares one. Put the "
                + "condition inside the trigger body, where it can test the inserted / deleted tables.");
        }

        var timing = Timing == TriggerTiming.Before
            ? throw new NotSupportedException(
                $"SQL Server has no BEFORE trigger, but trigger {Identifier} asks for one. Use INSTEAD OF, which "
                + "runs in place of the statement, or AFTER.")
            : TimingKeyword();

        return $"CREATE TRIGGER {Identifier} ON {Target} {timing} {EventList(", ")} AS {Body.Trim()}";
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TRIGGER IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.ToString()).ParameterName;

        builder.Append(
            "SELECT sm.definition, t.is_disabled FROM sys.sql_modules AS sm "
            + "INNER JOIN sys.triggers AS t ON t.object_id = sm.object_id "
            + $"WHERE t.object_id = OBJECT_ID(@{nameParam});");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readDefinitionAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        var existingIsDisabled = await reader.GetFieldValueAsync<bool>(1, ct).ConfigureAwait(false);

        return NormalizeBody(existing) == NormalizeBody(CreateStatement())
               && (IsDisabled is null || IsDisabled == existingIsDisabled)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<string?> FetchExistingDefinitionAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var definition = await readDefinitionAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return definition;
    }

    public async Task<bool?> FetchExistingIsDisabledAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        bool? isDisabled = await readDefinitionAsync(reader, ct).ConfigureAwait(false) == null
            ? null
            : await reader.GetFieldValueAsync<bool>(1, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return isDisabled;
    }

    public async Task<bool> ExistsInDatabaseAsync(SqlConnection conn, CancellationToken ct = default)
        => await FetchExistingDefinitionAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readDefinitionAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var definition = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(definition) ? null : definition;
    }
}
