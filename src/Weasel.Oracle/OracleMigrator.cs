using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Oracle;

public class OracleMigrator: Migrator
{
    public OracleMigrator(): base(OracleProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is OracleConnection;
    }

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine(CreateSchemaStatementFor(schemaName));
        }
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
        await createSchemas(migration, conn, logger, ct).ConfigureAwait(false);

        foreach (var delta in migration.Deltas)
        {
            var writer = new StringWriter();
            WriteUpdate(writer, delta);

            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $"@{scriptName}";
    }

    public override void AssertValidIdentifier(string name)
    {
        if (name.Length > 128)
        {
            throw new InvalidOperationException($"Oracle identifiers cannot exceed 128 characters. '{name}' is {name.Length} characters.");
        }
    }

    private static async Task createSchemas(
        SchemaMigration migration,
        DbConnection conn,
        IMigrationLogger logger,
        CancellationToken ct = default)
    {
        var writer = new StringWriter();

        if (migration.Schemas.Any())
        {
            new OracleMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var cmd = conn.CreateCommand(writer.ToString());
        logger.SchemaChange(cmd.CommandText);

        try
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            if (logger is DefaultMigrationLogger)
            {
                throw;
            }

            logger.OnFailure(cmd, e);
        }
    }

    public static string CreateSchemaStatementFor(string schemaName)
    {
        return $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_users WHERE username = '{schemaName.ToUpperInvariant()}';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER {schemaName} IDENTIFIED BY ""temp_password"" QUOTA UNLIMITED ON USERS';
        EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO {schemaName}';
    END IF;
END;
/
";
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }
}
