using System.Data.Common;
using System.Globalization;
using FirebirdSql.Data.FirebirdClient;
using JasperFx.Core;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird.Triggers;

/// <summary>
///     A Firebird trigger on a table or a view. <see cref="TriggerBase.Body" /> is the PSQL after
///     <c>AS</c>: a <c>BEGIN … END</c> block, with any <c>DECLARE VARIABLE</c> lines before it. A body
///     that is neither is wrapped in <c>BEGIN … END</c>.
/// </summary>
/// <remarks>
///     <para>
///         Firebird keeps the body as source, <c>AS</c> included, and records the rest in
///         <c>RDB$TRIGGERS</c>: the relation, the timing and events packed into
///         <c>RDB$TRIGGER_TYPE</c>, and whether the trigger is active. A delta compares all of it, so a
///         trigger moved to another table, fired on other events or switched off reports an update.
///     </para>
///     <para>
///         Created and updated with <c>CREATE OR ALTER TRIGGER … ACTIVE</c>, which changes every one of
///         those in place. <c>ACTIVE</c> is written out because <c>CREATE OR ALTER</c> otherwise leaves an
///         inactive trigger inactive.
///     </para>
///     <para>
///         Firebird triggers are row-level, so <see cref="TriggerBase.ForEachRow" /> is not emitted.
///         There is no <c>WHEN</c> clause, no <c>INSTEAD OF</c> -- a <c>BEFORE</c> trigger on a view does
///         that job -- and no <c>TRUNCATE</c>; each is refused rather than dropped. Database and DDL
///         triggers are not modelled, as on MySQL and Oracle.
///     </para>
/// </remarks>
public class Trigger: TriggerBase
{
    public Trigger(string name, string target, string body)
        : this(FirebirdProvider.Instance.Parse(name), FirebirdProvider.Instance.Parse(target), body)
    {
    }

    public Trigger(DbObjectName identifier, DbObjectName target, string body)
        : base(FirebirdObjectName.From(identifier ?? throw new ArgumentNullException(nameof(identifier))),
            FirebirdObjectName.From(target ?? throw new ArgumentNullException(nameof(target))), body)
    {
    }

    /// <summary>
    ///     A trigger on <paramref name="target" />, named the way the table names itself: exactly as
    ///     written when it has <see cref="ITable.PreserveIdentifierCase" />, as an EF Core model's table
    ///     does.
    /// </summary>
    public Trigger(string name, Tables.Table target, string body)
        : this(FirebirdProvider.Instance.Parse(name), target, body)
    {
    }

    /// <inheritdoc cref="Trigger(string, Tables.Table, string)" />
    public Trigger(DbObjectName identifier, Tables.Table target, string body)
        : this(identifier, (target ?? throw new ArgumentNullException(nameof(target))).Identifier, body)
    {
        PreserveTargetCase = target.PreserveIdentifierCase;
    }

    /// <summary>
    ///     When true, the table or view the trigger fires on is named exactly as written: delimited in
    ///     the DDL and looked up in the catalog as it is, the way a table with
    ///     <see cref="ITable.PreserveIdentifierCase" /> names itself. Otherwise Firebird's rule applies,
    ///     and an undelimited name is folded to upper case. The constructors that take a
    ///     <see cref="Tables.Table" /> copy it from the table.
    /// </summary>
    public bool PreserveTargetCase { get; set; }

    /// <summary>
    ///     The table or view the trigger fires on, as DDL writes it.
    /// </summary>
    internal string QuotedTargetName => SchemaUtils.QuoteName(Target.Name, PreserveTargetCase);

    /// <summary>
    ///     The table or view the trigger fires on, as Firebird's catalog stores it.
    /// </summary>
    internal string TargetCatalogName => SchemaUtils.CatalogName(Target.Name, PreserveTargetCase);

    /// <summary>
    ///     The trigger's name as Firebird's catalog stores it.
    /// </summary>
    internal string CatalogName => SchemaUtils.CatalogName(Identifier.Name);

    /// <summary>
    ///     Whether the trigger was read out of the catalog switched off.
    /// </summary>
    internal bool IsInactive { get; private init; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        FirebirdObjectName.AssertDefaultSchema(Identifier.Schema, $"trigger {Identifier.Name}");
        FirebirdObjectName.AssertDefaultSchema(Target.Schema, $"trigger {Identifier.Name}'s table {Target.Name}");

        FirebirdScript.WritePsql(writer, CreateStatement());
    }

    /// <summary>
    ///     The single <c>CREATE OR ALTER TRIGGER</c> statement this trigger renders to. Public because it
    ///     is what delta comparison and diagnostics both want, the same way a view exposes
    ///     <c>ToBasicCreateViewSql</c>.
    /// </summary>
    public string CreateStatement()
        => $"CREATE OR ALTER TRIGGER {SchemaUtils.QuoteName(Identifier.Name)} FOR {QuotedTargetName} "
           + $"ACTIVE {timing()} {events()} AS\n{ActionBody()}";

    /// <summary>
    ///     The body as it is written after <c>AS</c>, and as <c>RDB$TRIGGER_SOURCE</c> keeps it: wrapped in
    ///     <c>BEGIN … END</c> unless it already is a block or starts with its declarations.
    /// </summary>
    public string ActionBody()
    {
        var body = Body.Trim().TrimEnd(';').TrimEnd();
        var first = PsqlLexer.Tokenize(body).FirstOrDefault();

        return first.IsWord(body, "BEGIN") || first.IsWord(body, "DECLARE") ? body : $"BEGIN {body}; END";
    }

    private string timing()
    {
        if (Condition.IsNotEmpty())
        {
            throw new NotSupportedException(
                $"Firebird has no WHEN clause on a trigger, but trigger {Identifier} declares one. Put the "
                + "condition inside the trigger body.");
        }

        return Timing == TriggerTiming.InsteadOf
            ? throw new NotSupportedException(
                $"Firebird has no INSTEAD OF trigger, but trigger {Identifier} asks for one. A BEFORE trigger on "
                + "a view does the same job: it runs the statement's work itself.")
            : TimingKeyword();
    }

    private string events()
    {
        if (Events.HasFlag(TriggerEvents.Truncate))
        {
            throw new NotSupportedException(
                $"Firebird has no TRUNCATE, so it has no trigger on one, but trigger {Identifier} asks for it.");
        }

        return EventList(" OR ");
    }

    /// <summary>
    ///     A <c>DROP TRIGGER</c> that runs only while the trigger exists. Firebird before 6 has no
    ///     <c>DROP TRIGGER IF EXISTS</c>.
    /// </summary>
    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.Trigger(CatalogName),
            $"DROP TRIGGER {SchemaUtils.QuoteName(Identifier.Name)}");
    }

    /// <summary>
    ///     Firebird executes one statement per command, so introspection goes through
    ///     <see cref="FirebirdDbCommandBuilder" />.
    /// </summary>
    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    /// <summary>
    ///     The trigger's source, relation, type and state. Unterminated, because Firebird runs one
    ///     statement per command.
    /// </summary>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        // A name the server's catalog cannot hold would fail the query with "string truncation".
        FirebirdMigrator.AssertCatalogCanHold(CatalogName, FirebirdServerVersion.Of(builder.Command.Connection),
            "the name of a trigger");

        var name = builder.AddParameter(CatalogName).ParameterName;

        builder.Append(
            "SELECT RDB$TRIGGER_SOURCE, TRIM(RDB$RELATION_NAME), RDB$TRIGGER_TYPE, COALESCE(RDB$TRIGGER_INACTIVE, 0) "
            + $"FROM RDB$TRIGGERS WHERE RDB$TRIGGER_NAME = @{name}");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var actual = await readAsync(reader, ct).ConfigureAwait(false);

        if (actual == null)
        {
            return new CreateOrAlterDelta(this, null, SchemaPatchDifference.Create);
        }

        var differences = new List<string>();

        if (!string.Equals(TargetCatalogName, actual.TargetCatalogName, StringComparison.Ordinal))
        {
            differences.Add($"fires on {TargetCatalogName} rather than {actual.TargetCatalogName}");
        }

        if (Timing != actual.Timing || Events != actual.Events)
        {
            differences.Add($"fires {describe(Timing, Events)} rather than {describe(actual.Timing, actual.Events)}");
        }

        if (actual.IsInactive)
        {
            differences.Add("is inactive");
        }

        if (PsqlLexer.NormalizeSource(ActionBody()) != PsqlLexer.NormalizeSource(actual.Body))
        {
            differences.Add("the body differs");
        }

        return new CreateOrAlterDelta(this, actual,
            differences.Count == 0 ? SchemaPatchDifference.None : SchemaPatchDifference.Update,
            differences: differences);
    }

    private static string describe(TriggerTiming timing, TriggerEvents events)
        => $"{timing.ToString().ToUpperInvariant()} {events.ToString().ToUpperInvariant().Replace(", ", " OR ")}";

    /// <summary>
    ///     The trigger as the database has it, or null when there is no trigger of this name.
    /// </summary>
    public async Task<Trigger?> FetchExistingAsync(FbConnection conn, CancellationToken ct = default)
    {
        var builder = new FirebirdDbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        return await readAsync(reader, ct).ConfigureAwait(false);
    }

    public async Task<bool> ExistsInDatabaseAsync(FbConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;

    private async Task<Trigger?> readAsync(DbDataReader reader, CancellationToken ct)
        => await reader.ReadAsync(ct).ConfigureAwait(false) ? fromRow(reader) : null;

    private Trigger fromRow(DbDataReader reader)
    {
        var source = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();

        // A database trigger has no relation; its type decodes to no events, which is reported instead.
        var target = reader.IsDBNull(1) ? Target : new FirebirdObjectName(reader.GetString(1));
        var (timing, events) = Decode(Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture));

        // The catalog's spelling is exact, so a trigger read from it writes its table back exactly.
        return new Trigger(Identifier, target, afterAs(source))
        {
            PreserveTargetCase = !reader.IsDBNull(1) || PreserveTargetCase,
            Timing = timing,
            Events = events,
            IsInactive = Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture) == 1
        };
    }

    /// <summary>
    ///     <c>RDB$TRIGGER_SOURCE</c> keeps the <c>AS</c> that opens the body; the model's body starts after
    ///     it.
    /// </summary>
    private static string afterAs(string source)
    {
        var first = PsqlLexer.Tokenize(source).FirstOrDefault();
        return first.IsWord(source, "AS") ? source[first.End..].Trim() : source;
    }

    /// <summary>
    ///     Unpack <c>RDB$TRIGGER_TYPE</c>. For a table trigger it is <c>slots - 1 + (after ? 1 : 0)</c>,
    ///     where each of up to three two-bit slots, from bit 1 up, holds an event in the order it was
    ///     written -- 1 insert, 2 update, 3 delete. The order is not part of what a trigger means, so the
    ///     events come back as a set: <c>UPDATE OR INSERT</c> (11) and <c>INSERT OR UPDATE</c> (17) are
    ///     the same trigger.
    /// </summary>
    /// <remarks>
    ///     A database or DDL trigger has a type from 8192 up, and decodes to no events at all, which no
    ///     model matches.
    /// </remarks>
    internal static (TriggerTiming Timing, TriggerEvents Events) Decode(long type)
    {
        if (type is <= 0 or >= 8192)
        {
            return (TriggerTiming.Before, TriggerEvents.None);
        }

        var after = ((type + 1) & 1) == 1;
        var slots = type + 1 - (after ? 1 : 0);
        var events = TriggerEvents.None;

        for (var shift = 1; shift <= 5; shift += 2)
        {
            events |= ((slots >> shift) & 3) switch
            {
                1 => TriggerEvents.Insert,
                2 => TriggerEvents.Update,
                3 => TriggerEvents.Delete,
                _ => TriggerEvents.None
            };
        }

        return (after ? TriggerTiming.After : TriggerTiming.Before, events);
    }
}
