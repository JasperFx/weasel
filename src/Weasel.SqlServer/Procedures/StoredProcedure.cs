using System.Data.Common;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Procedures;

/// <summary>
///     A SQL Server stored procedure.
/// </summary>
/// <remarks>
///     This was the only stored procedure implementation in the tree, and it implemented
///     <see cref="ISchemaObject" /> directly rather than deriving from a shared base — so a second
///     provider had nothing to reuse. weasel#451 lifted the shared parts into
///     <see cref="StoredProcedureBase" /> and refitted this onto them, without changing what it
///     emits or how it compares.
/// </remarks>
public class StoredProcedure: StoredProcedureBase
{
    public StoredProcedure(DbObjectName identifier): base(identifier)
    {
    }

    public StoredProcedure(DbObjectName identifier, string body): base(identifier, body)
    {
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"drop procedure if exists {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        builder.Append($@"
select
    sys.sql_modules.definition
from sys.sql_modules
inner join sys.objects on sys.sql_modules.object_id = sys.objects.object_id
inner join sys.schemas on sys.objects.schema_id = sys.schemas.schema_id
where
    sys.objects.name = '{SchemaUtils.EscapeLiteral(Identifier.Name)}' and
    sys.schemas.name = '{SchemaUtils.EscapeLiteral(Identifier.Schema)}'
");
    }

    /// <inheritdoc />
    protected override ISchemaObjectDelta CreateDelta(string? existing)
        => new StoredProcedureDelta(this, existing == null ? null : new StoredProcedure(Identifier, existing));

    /// <summary>
    ///     <c>CREATE OR ALTER PROCEDURE</c>, which SQL Server has and the other three providers
    ///     spell differently or not at all.
    /// </summary>
    public void WriteCreateOrAlterStatement(Migrator rules, TextWriter writer)
    {
        var body = BodyText()
            .Replace("CREATE PROCEDURE", "CREATE OR ALTER PROCEDURE")
            .Replace("create procedure", "create or alter procedure");

        writer.WriteLine(body);
    }

    public async Task<StoredProcedure?> FetchExistingAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await ReadExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body == null ? null : new StoredProcedure(Identifier, body);
    }

    public async Task<StoredProcedureDelta> FindDeltaAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var actual = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new StoredProcedureDelta(this, actual);
    }
}
