using System.Data.Common;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql.Triggers;

/// <summary>
///     A PostgreSQL trigger.
/// </summary>
/// <remarks>
///     <para>
///         PostgreSQL is the odd one out: a trigger does not carry a body, it names a function to
///         execute. So <see cref="TriggerBase.Body" /> here is the function call —
///         <c>audit_orders()</c> — and the function has to exist, which means a PostgreSQL trigger
///         composes with <c>Weasel.Postgresql.Functions.Function</c> rather than duplicating it.
///     </para>
///     <para>
///         <c>pg_get_triggerdef</c> hands back the whole <c>CREATE TRIGGER</c> statement in the
///         server's own rendering, so the delta compares against a canonicalized form of that rather
///         than against the submitted text.
///     </para>
/// </remarks>
public class Trigger: TriggerBase
{
    public Trigger(string name, string target, string functionCall)
        : this(
            DbObjectName.Parse(PostgresqlProvider.Instance, name),
            DbObjectName.Parse(PostgresqlProvider.Instance, target),
            functionCall)
    {
    }

    public Trigger(DbObjectName identifier, DbObjectName target, string functionCall)
        : base(identifier, target, functionCall)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        // No CREATE OR REPLACE TRIGGER before PostgreSQL 14, and Weasel supports older, so drop
        // first. The drop is guarded, which also makes the create idempotent.
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
        var scope = ForEachRow ? "FOR EACH ROW" : "FOR EACH STATEMENT";
        var condition = Condition.IsNotEmpty() ? $" WHEN ({Condition})" : string.Empty;
        var call = Body.Trim().TrimEnd(';');

        return
            $"CREATE TRIGGER {SchemaUtils.QuoteName(Identifier.Name)} {TimingKeyword()} {EventList(" OR ")} "
            + $"ON {Target} {scope}{condition} EXECUTE FUNCTION {call};";
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TRIGGER IF EXISTS {SchemaUtils.QuoteName(Identifier.Name)} ON {Target};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        var tableParam = builder.AddParameter(Target.Name).ParameterName;
        var schemaParam = builder.AddParameter(Target.Schema).ParameterName;

        builder.Append($@"
SELECT pg_get_triggerdef(t.oid)
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT t.tgisinternal
  AND t.tgname = :{nameParam}
  AND c.relname = :{tableParam}
  AND n.nspname = :{schemaParam};");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readDefinitionAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return Matches(existing)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     Compare against <c>pg_get_triggerdef</c>'s rendering, which differs from the submitted
    ///     text in ways that carry no meaning: it always qualifies the table, always spells the
    ///     action as <c>EXECUTE FUNCTION</c>, and drops the quoting from names that do not need it.
    /// </summary>
    public bool Matches(string definition)
    {
        var actual = NormalizeBody(definition);
        var expected = NormalizeBody(CreateStatement());

        // pg_get_triggerdef renders the table qualified whether or not the caller did, so compare
        // on the tail after the ON clause plus the leading timing and event list.
        return actual == expected || actual == NormalizeBody(CreateStatement().Replace($"ON {Target}", $"ON {Target.QualifiedName}"));
    }

    public async Task<string?> FetchExistingDefinitionAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var definition = await readDefinitionAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return definition;
    }

    public async Task<bool> ExistsInDatabaseAsync(NpgsqlConnection conn, CancellationToken ct = default)
        => await FetchExistingDefinitionAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readDefinitionAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var definition = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(definition) ? null : definition;
    }
}
