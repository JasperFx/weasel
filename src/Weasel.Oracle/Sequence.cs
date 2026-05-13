using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle;

public class Sequence: SequenceBase
{
    public Sequence(string identifier)
        : base(DbObjectName.Parse(OracleProvider.Instance, identifier))
    {
    }

    public Sequence(DbObjectName identifier) : base(identifier)
    {
    }

    public Sequence(DbObjectName identifier, long startWith) : base(identifier, startWith)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var startsWith = StartWith ?? 1;

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

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
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

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;
        builder.Append(
            $"SELECT COUNT(*) FROM all_sequences WHERE sequence_owner = :{schemaParam} AND sequence_name = :{nameParam}");
    }

    // No CreateDeltaAsync / ReadExistsCountAsync override needed — base class's
    // Convert.ToInt64-based reader handles Oracle's decimal COUNT(*) natively.

    /// <summary>
    ///     Oracle-specific overload accepting an <see cref="OracleConnection" />. Forwards
    ///     to the base <see cref="SchemaObjectBase.FindDeltaAsync(DbConnection, CancellationToken)" />.
    /// </summary>
    public Task<ISchemaObjectDelta> FindDeltaAsync(OracleConnection conn, CancellationToken ct = default)
        => FindDeltaAsync((DbConnection)conn, ct);
}
