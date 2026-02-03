using System.Data.Common;
using Weasel.Core;

namespace Weasel.MySql;

public class Sequence: ISchemaObject
{
    public Sequence(DbObjectName identifier)
    {
        Identifier = identifier;
    }

    public Sequence(string sequenceName): this(DbObjectName.Parse(MySqlProvider.Instance, sequenceName))
    {
    }

    public DbObjectName Identifier { get; }

    public long StartWith { get; set; } = 1;
    public long IncrementBy { get; set; } = 1;

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        // MySQL 8.0+ supports sequences, but older versions don't
        // Using a table-based sequence for broader compatibility
        writer.WriteLine($"CREATE TABLE IF NOT EXISTS {Identifier.QualifiedName} (");
        writer.WriteLine("    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,");
        writer.WriteLine($"    current_value BIGINT NOT NULL DEFAULT {StartWith}");
        writer.WriteLine(");");
        writer.WriteLine($"INSERT IGNORE INTO {Identifier.QualifiedName} (current_value) VALUES ({StartWith});");
    }

    public void WriteDropStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"DROP TABLE IF EXISTS {Identifier.QualifiedName};");
    }

    public void ConfigureQueryCommand(Core.DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append($@"
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = @{schemaParam} AND table_name = @{nameParam};
");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var exists = false;

        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            exists = await reader.GetFieldValueAsync<long>(0, ct).ConfigureAwait(false) > 0;
        }

        return new SchemaObjectDelta(this, exists ? SchemaPatchDifference.None : SchemaPatchDifference.Create);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }
}
