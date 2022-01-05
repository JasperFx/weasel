using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Weasel.Core;

namespace Weasel.SqlServer
{
    public static class SchemaObjectsExtensions
    {


        internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
        {
            return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
        }

        public static async Task ApplyChanges(this ISchemaObject schemaObject, SqlConnection conn)
        {
            var migration = await SchemaMigration.Determine(conn, schemaObject).ConfigureAwait(false);

            await new SqlServerMigrator().ApplyAll(conn, migration, AutoCreate.CreateOrUpdate).ConfigureAwait(false);
        }

        public static Task Drop(this ISchemaObject schemaObject, SqlConnection conn)
        {
            var writer = new StringWriter();
            schemaObject.WriteDropStatement(new SqlServerMigrator(), writer);

            return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
        }

        public static Task Create(this ISchemaObject schemaObject, SqlConnection conn)
        {
            var writer = new StringWriter();
            schemaObject.WriteCreateStatement(new SqlServerMigrator(), writer);

            return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
        }

        public static async Task EnsureSchemaExists(this SqlConnection conn, string schemaName,
            CancellationToken cancellation = default)
        {
            var shouldClose = false;
            if (conn.State != ConnectionState.Open)
            {
                shouldClose = true;
                await conn.OpenAsync(cancellation).ConfigureAwait(false);
            }

            try
            {
                var sql = $@"
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{schemaName}' )
    EXEC('CREATE SCHEMA [{schemaName}]');

";

                await conn
                    .CreateCommand(sql)
                    .ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);
            }
            finally
            {
                if (shouldClose)
                {
                    await conn.CloseAsync().ConfigureAwait(false);
                }
            }
        }

        public static Task<IReadOnlyList<string?>> ActiveSchemaNames(this SqlConnection conn)
        {
            return conn.CreateCommand("select name from sys.schemas order by name")
                .FetchList<string>();
        }


        public static async Task DropSchema(this SqlConnection conn, string schemaName)
        {
            var procedures = await conn
                .CreateCommand($"select routine_name from information_schema.routines where routine_schema = '{schemaName}';")
                .FetchList<string>().ConfigureAwait(false);

            var constraints = await conn.CreateCommand($"select table_name, constraint_name from information_schema.table_constraints where table_schema = '{schemaName}' order by constraint_type").FetchList<string>(async r =>
            {
                var tableName = await r.GetFieldValueAsync<string>(0).ConfigureAwait(false);
                var constraintName = await r.GetFieldValueAsync<string>(1).ConfigureAwait(false);

                return $"alter table {schemaName}.{tableName} drop constraint {constraintName};";
            }).ConfigureAwait(false);

            var tables = await conn.CreateCommand($"select table_name from information_schema.tables where table_schema = '{schemaName}'").FetchList<string>().ConfigureAwait(false);

            var sequences = await conn
                .CreateCommand($"select sequence_name from information_schema.sequences where sequence_schema = '{schemaName}'")
                .FetchList<string>().ConfigureAwait(false);

            var tableTypes = await conn
                .CreateCommand(
                    $"select sys.table_types.name from sys.table_types inner join sys.schemas on sys.table_types.schema_id = sys.schemas.schema_id where sys.schemas.name = '{schemaName}'")
                .FetchList<string>().ConfigureAwait(false);

            var drops = new List<string>();
            drops.AddRange(procedures.Select(name => $"drop procedure {schemaName}.{name};"));
            drops.AddRange(constraints);
            drops.AddRange(tables.Select(name => $"drop table {schemaName}.{name};"));
            drops.AddRange(sequences.Select(name => $"drop sequence {schemaName}.{name};"));
            drops.AddRange(tableTypes.Select(x => $"DROP TYPE {schemaName}.{x};"));


            foreach (var drop in drops)
            {
                await conn.CreateCommand(drop).ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            if (!schemaName.EqualsIgnoreCase(SqlServerProvider.Instance.DefaultDatabaseSchemaName))
            {
                var sql = $"drop schema if exists {schemaName};";
                await conn.CreateCommand(sql).ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }

        public static Task CreateSchema(this SqlConnection conn, string schemaName)
        {
            return conn.CreateCommand(SqlServerMigrator.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync();
        }

        public static async Task ResetSchema(this SqlConnection conn, string schemaName)
        {
            try
            {
                await conn.DropSchema(schemaName).ConfigureAwait(false);
            }
            catch (SqlException e)
            {
                if (e.Message.Contains("deadlocked"))
                {
                    await Task.Delay(100).ConfigureAwait(false);
                    await conn.CloseAsync().ConfigureAwait(false);
                    await conn.OpenAsync().ConfigureAwait(false);
                    await conn.DropSchema(schemaName).ConfigureAwait(false);
                }
                else
                {
                    throw;
                }
            }
            await conn.RunSql(SqlServerMigrator.CreateSchemaStatementFor(schemaName)).ConfigureAwait(false);
        }

        public static async Task<bool> FunctionExists(this SqlConnection conn, DbObjectName functionIdentifier)
        {
            var sql =
                "SELECT specific_schema, routine_name FROM information_schema.routines WHERE type_udt_name != 'trigger' and routine_name like :name and specific_schema = :schema;";

            using var reader = await conn.CreateCommand(sql)
                .With("name", functionIdentifier.Name)
                .With("schema", functionIdentifier.Schema)
                .ExecuteReaderAsync().ConfigureAwait(false);

            return await reader.ReadAsync().ConfigureAwait(false);
        }

        public static async Task<IReadOnlyList<DbObjectName>> ExistingTables(this SqlConnection conn,
            string? namePattern = null)
        {
            var builder = new CommandBuilder();
            builder.Append("SELECT table_schema, table_name FROM information_schema.tables");


            if (namePattern.IsNotEmpty())
            {
                builder.Append(" WHERE table_name like @table");
                builder.AddNamedParameter("table", namePattern);
            }

            builder.Append(";");

            return await builder.FetchList(conn, ReadDbObjectName).ConfigureAwait(false);
        }

        public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctions(this SqlConnection conn,
            string? namePattern = null, string[]? schemas = null)
        {
            var builder = new CommandBuilder();
            builder.Append(
                "SELECT specific_schema, routine_name FROM information_schema.routines WHERE type_udt_name != 'trigger'");

            if (namePattern.IsNotEmpty())
            {
                builder.Append(" and routine_name like :name");
                builder.AddNamedParameter("name", namePattern);
            }

            if (schemas != null)
            {
                builder.Append(" and specific_schema = ANY(:schemas)");
                builder.AddNamedParameter("schemas", schemas);
            }

            builder.Append(";");

            return await builder.FetchList(conn, ReadDbObjectName).ConfigureAwait(false);
        }

        private static async Task<DbObjectName> ReadDbObjectName(DbDataReader reader)
        {
            return new(await reader.GetFieldValueAsync<string>(0).ConfigureAwait(false), await reader.GetFieldValueAsync<string>(1).ConfigureAwait(false));
        }

        /// <summary>
        ///     Write the creation SQL for this ISchemaObject
        /// </summary>
        /// <param name="object"></param>
        /// <param name="rules"></param>
        /// <returns></returns>
        public static string ToCreateSql(this ISchemaObject @object, SqlServerMigrator rules)
        {
            var writer = new StringWriter();
            @object.WriteCreateStatement(rules, writer);

            return writer.ToString();
        }
    }
}
