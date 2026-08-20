using System.Data.Common;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Triggers;

/// <summary>
///     An Oracle trigger. Oracle stores the trigger body verbatim, so the delta is a
///     whitespace-insensitive comparison, the same as its views.
/// </summary>
/// <remarks>
///     <c>CREATE OR REPLACE TRIGGER</c> rather than drop-then-create: it is idempotent, it is one
///     statement, and Oracle's migrator executes one statement per delta.
/// </remarks>
public class Trigger: TriggerBase
{
    public Trigger(string name, string target, string body)
        : this(
            DbObjectName.Parse(OracleProvider.Instance, name),
            DbObjectName.Parse(OracleProvider.Instance, target),
            body)
    {
    }

    public Trigger(DbObjectName identifier, DbObjectName target, string body): base(identifier, target, body)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine(CreateStatement());
    }

    /// <summary>
    ///     The single <c>CREATE TRIGGER</c> statement this trigger renders to. Public because it is
    ///     what delta comparison and diagnostics both want, the same way a view exposes
    ///     <c>ToBasicCreateViewSql</c>.
    /// </summary>
    public string CreateStatement()
    {
        var scope = ForEachRow ? " FOR EACH ROW" : string.Empty;
        var condition = Condition.IsNotEmpty() ? $" WHEN ({Condition})" : string.Empty;
        var body = Body.Trim();

        if (!body.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase)
            && !body.StartsWith("DECLARE", StringComparison.OrdinalIgnoreCase))
        {
            body = $"BEGIN {body.TrimEnd(';')}; END;";
        }

        return
            $"CREATE OR REPLACE TRIGGER {Identifier.QualifiedName} {TimingKeyword()} {EventList(" OR ")} "
            + $"ON {Target.QualifiedName}{scope}{condition} {body}";
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // No DROP TRIGGER IF EXISTS on Oracle, so swallow "trigger does not exist" the way the rest
        // of the provider does.
        writer.WriteLine($@"BEGIN
    EXECUTE IMMEDIATE 'DROP TRIGGER {SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -4080 THEN RAISE; END IF;
END;");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        if (builder.Command is OracleCommand oracleCommand)
        {
            // all_triggers.trigger_body is a LONG, and ODP.NET reads a LONG back empty by default.
            oracleCommand.InitialLONGFetchSize = -1;
        }

        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append(
            $"SELECT trigger_body FROM all_triggers WHERE owner = :{schemaParam} AND trigger_name = :{nameParam}");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readBodyAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new TriggerDelta(this, SchemaPatchDifference.Create);
        }

        // all_triggers stores only the body, not the header, so that is what gets compared.
        var expectedBody = CreateStatement();
        var index = expectedBody.IndexOf("BEGIN", StringComparison.OrdinalIgnoreCase);
        if (index < 0) index = expectedBody.IndexOf("DECLARE", StringComparison.OrdinalIgnoreCase);

        var expected = index < 0 ? expectedBody : expectedBody[index..];

        return NormalizeBody(existing) == NormalizeBody(expected)
            ? new TriggerDelta(this, SchemaPatchDifference.None)
            : new TriggerDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<string?> FetchExistingBodyAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await readBodyAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body;
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => await FetchExistingBodyAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readBodyAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var body = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(body) ? null : body;
    }
}

/// <summary>
///     Oracle applies a trigger change with <c>CREATE OR REPLACE TRIGGER</c> alone. The default
///     <see cref="SchemaObjectDelta" /> writes a DROP first, and Oracle's drop has to be an
///     anonymous PL/SQL block, so the pair arrives as a block followed by a DDL statement — which
///     ODP.NET cannot execute as one command, the same trap the Oracle view slice hit.
/// </summary>
internal class TriggerDelta: ISchemaObjectDelta
{
    private readonly Trigger _trigger;

    public TriggerDelta(Trigger trigger, SchemaPatchDifference difference)
    {
        _trigger = trigger;
        Difference = difference;
    }

    public ISchemaObject SchemaObject => _trigger;

    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer) => _trigger.WriteCreateStatement(rules, writer);

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
        => throw new NotSupportedException();
}
