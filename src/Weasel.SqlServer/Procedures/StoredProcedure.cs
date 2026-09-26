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
        var body = NormalizeCreateStatement(BodyText());

        AssertBodyCarriesNoBatchSeparator(Identifier, body);

        writer.WriteLine("GO");
        writer.WriteLine(body);
        writer.WriteLine("GO");
    }

    /// <summary>
    ///     Refuse a body that contains a line reading only <c>GO</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     This is the one hazard the <c>GO</c> bracketing carries, and without this check it is
    ///     silent. sqlcmd does not parse string literals and neither does
    ///     <see cref="SqlServerBatchSplitter" />, so a line reading only <c>GO</c> inside a body ends
    ///     the batch wherever it appears. The rendered script then splits in the middle of the
    ///     procedure: the fragment before the line is submitted as a complete definition, the
    ///     fragment after it as a statement of its own. What comes back is a syntax error pointing
    ///     at the tail of somebody's dynamic SQL, or -- worse -- a procedure that compiles and is
    ///     not the procedure that was written.
    ///     </para>
    ///     <para>
    ///     A body is user-authored T-SQL, and T-SQL that builds scripts is a normal thing to write,
    ///     so this is reachable rather than theoretical. Throwing at the point of emission puts the
    ///     failure where the cause is, naming the procedure, instead of somewhere downstream in a
    ///     file nobody has opened yet.
    ///     </para>
    ///     <para>
    ///     Internal and static so it can be exercised without a database, and so the
    ///     <see cref="Function" /> path -- which wraps its body in <c>EXEC sp_executesql</c> and is
    ///     therefore immune -- is not tempted to call it.
    ///     </para>
    /// </remarks>
    internal static void AssertBodyCarriesNoBatchSeparator(DbObjectName identifier, string body)
    {
        if (!SqlServerBatchSplitter.ContainsSeparator(body))
        {
            return;
        }

        throw new InvalidOperationException(
            $"The body of stored procedure {identifier.QualifiedName} contains a line whose entire content is GO. "
            + "Weasel brackets procedure DDL with GO separators so that a rendered migration script runs under "
            + "sqlcmd or SSMS (weasel#593), and neither sqlcmd nor Weasel's own splitter parses string literals, so "
            + "that line would end the batch in the middle of this definition. Put the word on a line with something "
            + "else on it, build it from pieces, or execute the statement through EXEC instead.");
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
