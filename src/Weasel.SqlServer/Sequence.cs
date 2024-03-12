using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer;

public class Sequence: ISchemaObject
{
    private readonly long? _startWith;

    public Sequence(string identifier)
    {
        Identifier = DbObjectName.Parse(SqlServerProvider.Instance, identifier);
    }

    public Sequence(DbObjectName identifier)
    {
        Identifier = identifier;
    }

    public Sequence(DbObjectName identifier, long startWith)
    {
        Identifier = identifier;
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
        var startsWith = _startWith ?? 1;

        writer.WriteLine(
            $"CREATE SEQUENCE {Identifier} START WITH {startsWith};");

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
            $"select count(*) from sys.sequences inner join sys.schemas on sys.sequences.schema_id = sys.schemas.schema_id where sys.schemas.name = @{schemaParam} and sys.sequences.name = @{nameParam};");
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

    public async Task<ISchemaObjectDelta> FindDeltaAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);

        var result = await CreateDeltaAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }
}
