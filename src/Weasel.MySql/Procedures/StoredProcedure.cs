using JasperFx.Core;
using MySqlConnector;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.MySql.Procedures;

/// <summary>
///     A MySQL stored procedure.
/// </summary>
/// <remarks>
///     <para>
///         MySQL stores a routine's body verbatim in
///         <c>information_schema.ROUTINES.ROUTINE_DEFINITION</c> — unlike a view definition, which
///         it rewrites — so comparison is a straight canonicalized match on the body. The stored
///         value is the body alone, without the <c>CREATE PROCEDURE …</c> header, so the caller's
///         statement is trimmed to its <c>BEGIN</c> before comparing.
///     </para>
///     <para>
///         There is no <c>CREATE OR REPLACE PROCEDURE</c>, so a change is a drop followed by a
///         create. Both go to the server in one command; MySqlConnector executes them in order,
///         which is what weasel#452 stopped the migrator from breaking by splitting delta SQL on
///         semicolons.
///     </para>
/// </remarks>
public class StoredProcedure: StoredProcedureBase
{
    public StoredProcedure(string name, string body)
        : this(DbObjectName.Parse(MySqlProvider.Instance, name), body)
    {
    }

    public StoredProcedure(DbObjectName identifier, string body): base(identifier, body)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        WriteDropStatement(migrator, writer);
        writer.WriteLine(BodyText());
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP PROCEDURE IF EXISTS {QualifiedName()};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append(
            "SELECT routine_definition FROM information_schema.ROUTINES "
            + $"WHERE routine_type = 'PROCEDURE' AND routine_schema = @{schemaParam} AND routine_name = @{nameParam}");
    }

    /// <inheritdoc />
    protected override bool Matches(string existing)
        => Canonicize(existing).Equals(Canonicize(ExtractBody(BodyText())), StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     Everything from the first <c>BEGIN</c>, which is what <c>ROUTINE_DEFINITION</c> stores.
    /// </summary>
    internal static string ExtractBody(string statement)
    {
        var index = statement.IndexOf("BEGIN", StringComparison.OrdinalIgnoreCase);
        return index < 0 ? statement : statement[index..].TrimEnd().TrimEnd(';');
    }

    private string QualifiedName()
        => $"{SchemaUtils.QuoteName(Identifier.Schema)}.{SchemaUtils.QuoteName(Identifier.Name)}";

    public async Task<string?> FetchExistingBodyAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var body = await ReadExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return body;
    }

    public async Task<bool> ExistsInDatabaseAsync(MySqlConnection conn, CancellationToken ct = default)
        => await FetchExistingBodyAsync(conn, ct).ConfigureAwait(false) != null;
}
