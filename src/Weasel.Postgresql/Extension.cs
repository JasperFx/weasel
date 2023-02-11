using System.Data.Common;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql;

/// <summary>
///     Used to register Postgresql extensions
/// </summary>
public class Extension: ISchemaObject
{
    public Extension(string extensionName)
    {
        ExtensionName = extensionName.Trim().ToLower();
    }

    public string ExtensionName { get; }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"CREATE EXTENSION IF NOT EXISTS {ExtensionName};");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP EXTENSION IF EXISTS {ExtensionName} CASCADE;");
    }

    public DbObjectName Identifier => new("public", ExtensionName);

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        builder.Append("select extname from pg_extension where extname = ");
        builder.AppendParameter(ExtensionName);
        builder.Append(";");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var exists = await reader.ReadAsync(ct).ConfigureAwait(false);

        return new SchemaObjectDelta(this, exists ? SchemaPatchDifference.None : SchemaPatchDifference.Create);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }
}
