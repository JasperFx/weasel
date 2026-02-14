using System.Data.Common;
using Npgsql;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql;

public class Sequence: ISchemaObject
{
    private readonly long? _startWith;

    public Sequence(string identifier)
    {
        Identifier = DbObjectName.Parse(PostgresqlProvider.Instance, identifier);
    }

    public Sequence(DbObjectName identifier)
    {
        Identifier = PostgresqlObjectName.From(identifier);
    }

    public Sequence(DbObjectName identifier, long startWith)
    {
        Identifier = PostgresqlObjectName.From(identifier);
        _startWith = startWith;
    }

    public DbObjectName? Owner { get; set; }
    public string OwnerColumn { get; set; } = null!;
    public DbObjectName Identifier { get; }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine(
            $"CREATE SEQUENCE {Identifier}{(_startWith.HasValue ? $" START {_startWith.Value}" : string.Empty)};");

        if (Owner != null)
        {
            writer.WriteLine($"ALTER SEQUENCE {Identifier} OWNED BY {Owner}.{OwnerColumn};");
        }
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP SEQUENCE IF EXISTS {Identifier};");
    }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        builder.Append(
            $"select count(*) from information_schema.sequences where sequence_schema = :{schemaParam} and sequence_name = :{nameParam};");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false) ||
            await reader.GetFieldValueAsync<int>(0, ct).ConfigureAwait(false) == 0)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return new SchemaObjectDelta(this, SchemaPatchDifference.None);
    }

    public async Task<ISchemaObjectDelta> FindDeltaAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);

        var result = await CreateDeltaAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }
}
