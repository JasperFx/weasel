using System.Data.Common;
using JasperFx.Core;
using MySqlConnector;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.MySql.Triggers;

/// <summary>
///     A MySQL trigger. MySQL stores the action statement verbatim in
///     <c>information_schema.TRIGGERS.ACTION_STATEMENT</c> — unlike a view, whose definition it
///     rewrites — so the delta is a whitespace-insensitive comparison of the body.
/// </summary>
/// <remarks>
///     <para>
///         MySQL triggers fire on exactly one event and are always row-level, so
///         <see cref="TriggerBase.Events" /> carrying more than one is an error rather than a
///         silently narrowed trigger, and <see cref="TriggerBase.ForEachRow" /> is not emitted.
///         There is no <c>WHEN</c> clause; the condition goes inside the body.
///     </para>
///     <para>
///         Creating a trigger needs the <c>TRIGGER</c> privilege, and on a server with binary
///         logging enabled it also needs <c>SUPER</c> or
///         <c>log_bin_trust_function_creators</c> — MySQL refuses otherwise with a message about
///         the SUPER privilege that does not mention triggers at all.
///     </para>
/// </remarks>
public class Trigger: TriggerBase
{
    public Trigger(string name, string target, string body)
        : this(
            DbObjectName.Parse(MySqlProvider.Instance, name),
            DbObjectName.Parse(MySqlProvider.Instance, target),
            body)
    {
    }

    public Trigger(DbObjectName identifier, DbObjectName target, string body): base(identifier, target, body)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        // No CREATE OR REPLACE TRIGGER on MySQL, so drop first. Both statements go to the server in
        // one command; MySqlConnector executes them in order.
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
        if (Condition.IsNotEmpty())
        {
            throw new NotSupportedException(
                $"MySQL has no WHEN clause on a trigger, but trigger {Identifier} declares one. Put the "
                + "condition inside the trigger body.");
        }

        if (Timing == TriggerTiming.InsteadOf)
        {
            throw new NotSupportedException(
                $"MySQL has no INSTEAD OF trigger, but trigger {Identifier} asks for one.");
        }

        return
            $"CREATE TRIGGER {QualifiedName(Identifier)} {TimingKeyword()} {SingleEvent("MySQL")} "
            + $"ON {QualifiedName(Target)} FOR EACH ROW {Body.Trim()}";
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TRIGGER IF EXISTS {QualifiedName(Identifier)};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append(
            "SELECT action_statement FROM information_schema.TRIGGERS "
            + $"WHERE trigger_schema = @{schemaParam} AND trigger_name = @{nameParam};");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readBodyAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return NormalizeBody(existing) == NormalizeBody(Body)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<string?> FetchExistingBodyAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await readBodyAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body;
    }

    public async Task<bool> ExistsInDatabaseAsync(MySqlConnection conn, CancellationToken ct = default)
        => await FetchExistingBodyAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readBodyAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var body = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(body) ? null : body;
    }

    private static string QualifiedName(DbObjectName name)
        => $"{SchemaUtils.QuoteName(name.Schema)}.{SchemaUtils.QuoteName(name.Name)}";
}
