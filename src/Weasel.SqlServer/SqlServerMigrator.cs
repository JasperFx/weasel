using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer
{
    public class SqlServerMigrator : Migrator
    {
        public SqlServerMigrator() : base(SqlServerProvider.Instance.DefaultDatabaseSchemaName)
        {
        }

        public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
        {
            writeStep(this, writer);
        }

        public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
        {
            foreach (var schemaName in schemaNames)
            {
                writer.WriteLine(SqlServerMigrator.CreateSchemaStatementFor(schemaName));
            }

        }

        private const string BeginScript = @"DO $$
BEGIN";

        private const string EndScript = @"END
$$;
";

        protected override async Task executeDelta(SchemaMigration migration, DbConnection conn, AutoCreate autoCreate, IMigrationLogger logger)
        {
            await createSchemas(migration, conn, logger).ConfigureAwait(false);


            foreach (var delta in migration.Deltas)
            {
                var writer = new StringWriter();
                WriteUpdate(writer, delta);

                if (writer.ToString().Trim().IsNotEmpty())
                {
                    await executeCommand(conn, logger, writer).ConfigureAwait(false);
                }
            }
        }

        public override string ToExecuteScriptLine(string scriptName)
        {
            return $":r {scriptName}";
        }

        public override void AssertValidIdentifier(string name)
        {
            // Nothing yet
        }

        private async Task createSchemas(Core.SchemaMigration migration, DbConnection conn,
            IMigrationLogger logger)
        {
            var writer = new StringWriter();

            if (migration.Schemas.Any())
            {
                new SqlServerMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
                if (writer.ToString().Trim().IsNotEmpty()) // Cheesy way of knowing if there is any delta
                {
                    await executeCommand(conn, logger, writer).ConfigureAwait(false);
                }
            }
        }

        private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer)
        {
            var cmd = conn.CreateCommand(writer.ToString());
            logger.SchemaChange(cmd.CommandText);

            try
            {
                await cmd
                    .ExecuteNonQueryAsync().ConfigureAwait(false);
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
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{schemaName}' )
    EXEC('CREATE SCHEMA [{schemaName}]');

";
        }
    }

}
