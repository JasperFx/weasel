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
    sys.schemas.name = '{SchemaUtils.EscapeLiteral(Identifier.Schema)}';
");
    }

    /// <inheritdoc />
    protected override ISchemaObjectDelta CreateDelta(string? existing)
        => new StoredProcedureDelta(this, existing == null ? null : new StoredProcedure(Identifier, existing));

    /// <summary>
    ///     The body with its leading <c>CREATE [OR ALTER] PROC[EDURE]</c> keyword rewritten to
    ///     <c>CREATE OR ALTER PROCEDURE</c>. Internal rather than private so it can be exercised
    ///     without a database.
    /// </summary>
    internal static string NormalizeCreateStatement(string body) => body.ToCreateOrAlterProcedure();

    /// <summary>
    ///     The create path and the update path emit the same thing, because there is only one form
    ///     that is safe to run twice. See <see cref="WriteCreateOrAlterStatement" />.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        writeBatchedBody(writer);
    }

    /// <summary>
    ///     <c>CREATE OR ALTER PROCEDURE</c>, which SQL Server has and the other three providers
    ///     spell differently or not at all.
    /// </summary>
    /// <remarks>
    ///     Bracketed by <c>GO</c> lines: SQL Server requires <c>CREATE OR ALTER PROCEDURE</c> to be
    ///     the first statement of its batch, and a rendered migration concatenates every object's
    ///     DDL into one script, so the separators are what make that script runnable at all
    ///     (weasel#593). They are written here rather than folded into the body so that
    ///     <see cref="StoredProcedureBase.BodyText" /> and
    ///     <see cref="StoredProcedureBase.CanonicizeSql" />, which are compared against
    ///     <c>sys.sql_modules</c>, never see them.
    /// </remarks>
    public void WriteCreateOrAlterStatement(Migrator rules, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        writeBatchedBody(writer);
    }

    private void writeBatchedBody(TextWriter writer)
    {
        writer.WriteLine("GO");
        writer.WriteLine(NormalizeCreateStatement(BodyText()));
        writer.WriteLine("GO");
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
