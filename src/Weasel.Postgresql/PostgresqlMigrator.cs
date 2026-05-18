using System.Data.Common;
using System.Text;
using JasperFx;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Migrations;
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

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine(SchemaObjectsExtensions.DropStatementFor(schemaName));
        }
    }

    private static void WriteSql(string databaseSchemaName, TextWriter writer)
    {
        // Neither the "IF NOT EXISTS(information_schema.schemata) THEN EXECUTE 'CREATE SCHEMA'"
        // pattern nor PostgreSQL's own "CREATE SCHEMA IF NOT EXISTS" is concurrent-safe — both
        // can have two sessions pass the existence check then race on the insert into pg_namespace,
        // surfacing as "23505 duplicate key value violates unique constraint pg_namespace_nspname_index"
        // / "42P06 schema X already exists". Wrap the create in a sub-block that swallows those two
        // race exceptions specifically; any other error still propagates.
        writer.WriteLine(
            $"""
                   BEGIN
                     EXECUTE 'CREATE SCHEMA IF NOT EXISTS {PostgresqlProvider.Instance.ToQualifiedName(databaseSchemaName)}';
                   EXCEPTION
                     WHEN duplicate_schema THEN NULL;
                     WHEN unique_violation THEN NULL;
                   END;

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
        return $"create schema if not exists {PostgresqlProvider.Instance.ToQualifiedName(schemaName)};";
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

    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
        var databaseName = builder.Database;

        if (string.IsNullOrEmpty(databaseName))
        {
            throw new ArgumentException("The connection string does not specify a database name.");
        }

        builder.Database = "postgres";
        await using var adminConn = new NpgsqlConnection(builder.ConnectionString);
        await adminConn.OpenAsync(ct).ConfigureAwait(false);

        if (!await adminConn.DatabaseExists(databaseName, ct).ConfigureAwait(false))
        {
            await new DatabaseSpecification().BuildDatabase(adminConn, databaseName, ct).ConfigureAwait(false);
        }
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public DatabaseWithTables CreateDatabase(NpgsqlDataSource dataSource)
    {
        var builder = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
        return new DatabaseWithTables(builder.Database ?? "weasel", dataSource);
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not NpgsqlConnection)
        {
            throw new ArgumentException("Expected NpgsqlConnection", nameof(connection));
        }

        var dataSource = new NpgsqlDataSourceBuilder(connection.ConnectionString).Build();
        var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.Database ?? "weasel", dataSource);
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        sb.Append("TRUNCATE TABLE ");
        sb.Append(string.Join(", ", tables.Select(t => t.QualifiedName)));

        if (resetIdentity)
        {
            sb.Append(" RESTART IDENTITY");
        }

        sb.Append(" CASCADE;");

        return sb.ToString();
    }

    public override IDatabaseWithTables CreateDatabase(DbDataSource dataSource, string? identifier = null)
    {
        if (dataSource is not NpgsqlDataSource npgsqlDataSource)
        {
            return base.CreateDatabase(dataSource, identifier);
        }

        var builder = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.Database ?? "weasel", npgsqlDataSource);
    }
}
