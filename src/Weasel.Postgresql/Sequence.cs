using Npgsql;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql;

public class Sequence: SequenceBase
{
    public Sequence(string identifier)
        : base(DbObjectName.Parse(PostgresqlProvider.Instance, identifier))
    {
    }

    public Sequence(DbObjectName identifier)
        : base(PostgresqlObjectName.From(identifier))
    {
    }

    public Sequence(DbObjectName identifier, long startWith)
        : base(PostgresqlObjectName.From(identifier), startWith)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine(
            $"CREATE SEQUENCE {Identifier}{(StartWith.HasValue ? $" START {StartWith.Value}" : string.Empty)};");

        if (Owner != null)
        {
            writer.WriteLine($"ALTER SEQUENCE {Identifier} OWNED BY {Owner}.{OwnerColumn};");
        }
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP SEQUENCE IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        builder.Append(
            $"select count(*) from information_schema.sequences where sequence_schema = :{schemaParam} and sequence_name = :{nameParam};");
    }

    /// <summary>
    ///     Provider-specific <see cref="Sequence" /> overload that accepts a
    ///     <see cref="NpgsqlConnection" /> for caller convenience. Forwards to the base
    ///     <see cref="SchemaObjectBase.FindDeltaAsync(System.Data.Common.DbConnection, CancellationToken)" />.
    /// </summary>
    public Task<ISchemaObjectDelta> FindDeltaAsync(NpgsqlConnection conn, CancellationToken ct = default)
        => FindDeltaAsync((System.Data.Common.DbConnection)conn, ct);
}
