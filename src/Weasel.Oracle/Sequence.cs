using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle;

public class Sequence: ISchemaObject
{
    private readonly long? _startWith;

    public Sequence(string identifier)
    {
        Identifier = DbObjectName.Parse(OracleProvider.Instance, identifier);
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

        // Use PL/SQL exception handling to safely create sequence if it doesn't exist
        // This avoids race conditions where another process creates the sequence
        // between our check and create
        writer.WriteLine($@"
BEGIN
    EXECUTE IMMEDIATE 'CREATE SEQUENCE {Identifier} START WITH {startsWith}';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            NULL; -- ORA-00955: name already used, ignore
        ELSE
            RAISE;
        END IF;
END;");
        writer.WriteLine("/");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_sequences WHERE sequence_name = '{Identifier.Name.ToUpperInvariant()}' AND sequence_owner = '{Identifier.Schema.ToUpperInvariant()}';
    IF v_count > 0 THEN
        EXECUTE IMMEDIATE 'DROP SEQUENCE {Identifier}';
    END IF;
END;
/
");
    }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;
        builder.Append(
            $"SELECT COUNT(*) FROM all_sequences WHERE sequence_owner = :{schemaParam} AND sequence_name = :{nameParam}");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false) ||
            await reader.GetFieldValueAsync<decimal>(0, ct).ConfigureAwait(false) == 0)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return new SchemaObjectDelta(this, SchemaPatchDifference.None);
    }

    public async Task<ISchemaObjectDelta> FindDeltaAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);

        var result = await CreateDeltaAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }
}
