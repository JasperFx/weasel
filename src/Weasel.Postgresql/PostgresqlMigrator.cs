using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql;

public class PostgresqlMigrator: Migrator
{
    private const string BeginScript = @"DO $$
BEGIN";

    private const string EndScript = @"END
$$;
";

    public PostgresqlMigrator(): base(PostgresqlProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    /// <summary>
    ///     Used to validate database object name lengths against Postgresql's NAMEDATALEN property to avoid
    ///     Marten getting confused when comparing database schemas against the configuration. See
    ///     https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html
    ///     for more information. This does NOT adjust NAMEDATALEN for you.
    /// </summary>
    public int NameDataLength { get; set; } = 64;

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is NpgsqlConnection;
    }

    public override IDatabaseProvider Provider => PostgresqlProvider.Instance;

    /// <summary>
    ///     Write out a templated SQL script with all rules
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="writeStep">A continuation to write the inner SQL</param>
    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        if (IsTransactional)
        {
            writer.WriteLine("DO LANGUAGE plpgsql $tran$");
            writer.WriteLine("BEGIN");
            writer.WriteLine("");
        }

        if (Role.IsNotEmpty())
        {
            writer.WriteLine($"SET ROLE {Role};");
            writer.WriteLine("");
        }

        writeStep(this, writer);

        if (Role.IsNotEmpty())
        {
            writer.WriteLine("RESET ROLE;");
            writer.WriteLine("");
        }

        if (IsTransactional)
        {
            writer.WriteLine("");
            writer.WriteLine("END;");
            writer.WriteLine("$tran$;");
        }
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        writer.Write(BeginScript);

        foreach (var schemaName in schemaNames) WriteSql(schemaName, writer);

        writer.WriteLine(EndScript);
        writer.WriteLine();
    }

    private static void WriteSql(string databaseSchemaName, TextWriter writer)
    {
        writer.WriteLine(
            $"""
                 IF NOT EXISTS(
                     SELECT schema_name
                       FROM information_schema.schemata
                       WHERE schema_name = '{databaseSchemaName}'
                   )
                 THEN
                   EXECUTE 'CREATE SCHEMA {PostgresqlProvider.Instance.ToQualifiedName(databaseSchemaName)}';
                 END IF;

             """);
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
    {
        var writer = new StringWriter();

        if (migration.Schemas.Any())
        {
            WriteSchemaCreationSql(migration.Schemas, writer);
        }

        migration.WriteAllUpdates(writer, this, autoCreate);

        var sqlCommands = writer
            .ToString()
            .Split(new[] { IndexDefinition.IndexCreationBeginComment, IndexDefinition.IndexCreationEndComment },
                StringSplitOptions.RemoveEmptyEntries);
        foreach (var sql in sqlCommands)
        {
            var cmd = conn.CreateCommand(sql);
            logger.SchemaChange(cmd.CommandText);

            try
            {
                await cmd
                    .ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $"\\i {scriptName}";
    }

    public static string CreateSchemaStatementFor(string schemaName)
    {
        Console.WriteLine("I AM WRITING OUT SCHEMA CREATION FOR " + schemaName);
        return $"create schema if not exists {schemaName};";
    }

    public override void AssertValidIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new PostgresqlIdentifierInvalidException(name);
        }

        if (name.IndexOf(' ') >= 0)
        {
            throw new PostgresqlIdentifierInvalidException(name);
        }

        if (name.Length < NameDataLength)
        {
            return;
        }

        throw new PostgresqlIdentifierTooLongException(NameDataLength, name);
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }
}
