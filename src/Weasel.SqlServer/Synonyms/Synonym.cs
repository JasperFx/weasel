using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Synonyms;

/// <summary>
///     A SQL Server synonym: a second name for an object, possibly in another database or on a
///     linked server.
/// </summary>
/// <remarks>
///     <para>
///         The target is stored as text and is deliberately not validated — a synonym may point at
///         something that does not exist yet, or at a four-part name on a linked server, and SQL
///         Server accepts both. <c>sys.synonyms.base_object_name</c> hands the name back bracketed
///         and fully qualified whatever the caller wrote, so comparison normalizes the brackets away
///         rather than demanding the caller match SQL Server's spelling.
///     </para>
///     <para>
///         There is no <c>ALTER SYNONYM</c>, so a change is a drop and a create.
///     </para>
/// </remarks>
public class Synonym: SchemaObjectBase
{
    public Synonym(string name, string target)
        : this(DbObjectName.Parse(SqlServerProvider.Instance, name), target)
    {
    }

    public Synonym(DbObjectName identifier, string target): base(identifier)
    {
        Target = target ?? throw new ArgumentNullException(nameof(target));
    }

    /// <summary>The object this synonym stands for.</summary>
    public string Target { get; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);
        writer.WriteLine($"CREATE SYNONYM {Identifier} FOR {Target};");
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP SYNONYM IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;

        builder.Append(
            "SELECT s.base_object_name FROM sys.synonyms s "
            + "INNER JOIN sys.schemas sch ON sch.schema_id = s.schema_id "
            + $"WHERE s.name = @{nameParam} AND sch.name = @{schemaParam};");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readTargetAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return Normalize(existing) == Normalize(Target)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     <c>sys.synonyms</c> brackets and qualifies the target whatever the caller wrote, so the
    ///     brackets come off both sides before comparing. Case-insensitive, like SQL Server itself.
    /// </summary>
    internal static string Normalize(string target)
        => target.Replace("[", "").Replace("]", "").Trim().ToUpperInvariant();

    public async Task<string?> FetchExistingTargetAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var target = await readTargetAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return target;
    }

    public async Task<bool> ExistsInDatabaseAsync(SqlConnection conn, CancellationToken ct = default)
        => await FetchExistingTargetAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readTargetAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var target = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(target) ? null : target;
    }
}
