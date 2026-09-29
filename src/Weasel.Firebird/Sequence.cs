using System.Data.Common;
using System.Globalization;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird;

/// <summary>
///     A Firebird sequence (generator), for Firebird 3, 4 and 5.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="SequenceBase.StartWith" /> is the first value <c>NEXT VALUE FOR</c> returns, on every
///         version. Firebird 3 and 4 read <c>START WITH</c> differently: 3 sets the sequence's current
///         value, so the first value handed out is one increment past it; 4 and later make it the first
///         value handed out. DDL is written without knowing the server, so the create asks the engine
///         which it is and passes 3 the value one increment lower.
///     </para>
///     <para>
///         An increment the model states is compared, and changed in place with <c>ALTER SEQUENCE</c> --
///         the value the sequence has reached is kept. Dropping and recreating it, as a plain existence
///         delta would, restarts it and hands out values already used. The start value is not compared:
///         the catalog keeps where a sequence has got to, not where it began.
///     </para>
/// </remarks>
public class Sequence: SequenceBase
{
    public Sequence(string identifier): this(FirebirdProvider.Instance.Parse(identifier))
    {
    }

    public Sequence(DbObjectName identifier): base(FirebirdObjectName.From(identifier))
    {
    }

    public Sequence(DbObjectName identifier, long startWith): base(FirebirdObjectName.From(identifier), startWith)
    {
    }

    internal string QuotedName => SchemaUtils.QuoteName(Identifier.Name);

    internal string CatalogName => SchemaUtils.CatalogName(Identifier.Name);

    /// <summary>
    ///     A guarded <c>CREATE SEQUENCE</c>. Never <c>CREATE OR ALTER SEQUENCE</c>: without an option it
    ///     is a syntax error, and with one it would reset a sequence that is already in use.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        FirebirdObjectName.AssertDefaultSchema(Identifier.Schema, $"sequence {Identifier.Name}");

        var increment = IncrementBy ?? 1;
        if (increment == 0)
        {
            throw new InvalidOperationException($"Sequence {Identifier.Name} cannot have an increment of 0");
        }

        if (!StartWith.HasValue && increment == 1)
        {
            // Both versions hand out 1 first, so there is nothing to tell apart.
            FirebirdScript.WriteGuarded(writer, CatalogProbes.Sequence(CatalogName), $"CREATE SEQUENCE {QuotedName}");
            return;
        }

        var first = StartWith ?? 1;
        FirebirdScript.WritePsql(writer, $"""
            EXECUTE BLOCK AS
            BEGIN
              IF (NOT EXISTS({CatalogProbes.Sequence(CatalogName)})) THEN
                IF (rdb$get_context('SYSTEM', 'ENGINE_VERSION') STARTING WITH '3.') THEN
                  EXECUTE STATEMENT {FirebirdScript.StatementLiteral(createSql(first - increment, increment))};
                ELSE
                  EXECUTE STATEMENT {FirebirdScript.StatementLiteral(createSql(first, increment))};
            END
            """);
    }

    private string createSql(long startWith, long increment)
        => $"CREATE SEQUENCE {QuotedName} START WITH {startWith.ToString(CultureInfo.InvariantCulture)} INCREMENT BY {increment.ToString(CultureInfo.InvariantCulture)}";

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.Sequence(CatalogName), $"DROP SEQUENCE {QuotedName}");
    }

    internal void WriteAlterIncrement(TextWriter writer, long increment)
    {
        FirebirdScript.WriteStatement(writer,
            $"ALTER SEQUENCE {QuotedName} INCREMENT BY {increment.ToString(CultureInfo.InvariantCulture)}");
    }

    /// <summary>
    ///     Whether the sequence exists, and its increment. Unterminated: on Firebird every introspection
    ///     query is a command of its own. A name longer than the server's catalog holds is refused as over
    ///     the limit, before it is bound: the query would otherwise fail with "string truncation".
    /// </summary>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var version = builder is FirebirdDbCommandBuilder firebird
            ? firebird.ServerVersion
            : FirebirdServerVersion.Of(builder.Command.Connection);
        FirebirdMigrator.AssertCatalogCanHold(CatalogName, version, "the name of a sequence");

        var name = builder.AddParameter(CatalogName).ParameterName;
        builder.Append(
            $"SELECT RDB$GENERATOR_INCREMENT FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = @{name} AND COALESCE(RDB$SYSTEM_FLAG, 0) = 0");
    }

    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return new SequenceDelta(this, null);
        }

        var increment = await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)
            ? 1
            : Convert.ToInt64(await reader.GetFieldValueAsync<object>(0, ct).ConfigureAwait(false), CultureInfo.InvariantCulture);

        return new SequenceDelta(this, increment);
    }

    /// <summary>
    ///     Firebird-specific overload accepting an <see cref="FbConnection" />.
    /// </summary>
    public Task<ISchemaObjectDelta> FindDeltaAsync(FbConnection conn, CancellationToken ct = default)
        => FindDeltaAsync((DbConnection)conn, ct);
}

/// <summary>
///     A sequence that is missing, or whose increment differs from the one the model states -- which is
///     changed in place, keeping the value the sequence has reached.
/// </summary>
public class SequenceDelta: ISchemaObjectDelta
{
    private readonly Sequence _sequence;

    public SequenceDelta(Sequence sequence, long? actualIncrement)
    {
        _sequence = sequence;
        ActualIncrement = actualIncrement;

        Difference = actualIncrement == null
            ? SchemaPatchDifference.Create
            : sequence.IncrementBy.HasValue && sequence.IncrementBy != actualIncrement
                ? SchemaPatchDifference.Update
                : SchemaPatchDifference.None;
    }

    /// <summary>
    ///     The increment the catalog holds, or null when the sequence does not exist.
    /// </summary>
    public long? ActualIncrement { get; }

    public ISchemaObject SchemaObject => _sequence;

    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Create)
        {
            _sequence.WriteCreateStatement(rules, writer);
        }
        else if (Difference == SchemaPatchDifference.Update)
        {
            _sequence.WriteAlterIncrement(writer, _sequence.IncrementBy!.Value);
        }
    }

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Create)
        {
            _sequence.WriteDropStatement(rules, writer);
        }
        else if (Difference == SchemaPatchDifference.Update)
        {
            _sequence.WriteAlterIncrement(writer, ActualIncrement!.Value);
        }
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        if (ActualIncrement.HasValue)
        {
            _sequence.WriteAlterIncrement(writer, ActualIncrement.Value);
        }
    }

    public override string ToString() => $"SequenceDelta for {_sequence.Identifier}";
}
