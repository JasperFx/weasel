using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Weasel.Core;
using Weasel.SqlServer.Functions;

namespace Weasel.SqlServer
{
    public static class SchemaObjectsExtensions
    {
        public static Task<Function> FindExistingFunction(this SqlConnection conn, DbObjectName functionName)
        {
            var function = new Function(functionName, null);
            return function.FetchExisting(conn);
        }


        internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
        {
            return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
        }

        public static async Task ApplyChanges(this ISchemaObject schemaObject, SqlConnection conn)
        {
            var migration = await SchemaMigration.Determine(conn, schemaObject);

            await migration.ApplyAll(conn, new DdlRules(), AutoCreate.CreateOrUpdate);
        }

        public static Task Drop(this ISchemaObject schemaObject, SqlConnection conn)
        {
            var writer = new StringWriter();
            schemaObject.WriteDropStatement(new DdlRules(), writer);

            return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
        }

        public static Task Create(this ISchemaObject schemaObject, SqlConnection conn)
        {
            var writer = new StringWriter();
            schemaObject.WriteCreateStatement(new DdlRules(), writer);

            return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
        }

        public static async Task EnsureSchemaExists(this SqlConnection conn, string schemaName,
            CancellationToken cancellation = default)
        {
            var shouldClose = false;
            if (conn.State != ConnectionState.Open)
            {
                shouldClose = true;
                await conn.OpenAsync(cancellation);
            }

            try
            {
                await conn
                    .CreateCommand(SchemaMigration.CreateSchemaStatementFor(schemaName))
                    .ExecuteNonQueryAsync(cancellation);
            }
            finally
            {
                if (shouldClose)
                {
                    await conn.CloseAsync();
                }
            }
        }

        public static Task<IReadOnlyList<string>> ActiveSchemaNames(this SqlConnection conn)
        {
            return conn.CreateCommand("select nspname from pg_catalog.pg_namespace order by nspname")
                .FetchList<string>();
        }


        public static Task DropSchema(this SqlConnection conn, string schemaName)
        {
            return conn.CreateCommand(DropStatementFor(schemaName)).ExecuteNonQueryAsync();
        }

        public static string DropStatementFor(string schemaName)
        {
            return $"drop schema if exists {schemaName};";
        }

        public static Task CreateSchema(this SqlConnection conn, string schemaName)
        {
            return conn.CreateCommand(SchemaMigration.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync();
        }

        public static async Task ResetSchema(this SqlConnection conn, string schemaName)
        {
            await conn.RunSql(DropStatementFor(schemaName));
            await conn.RunSql(SchemaMigration.CreateSchemaStatementFor(schemaName));
        }

        public static async Task<bool> FunctionExists(this SqlConnection conn, DbObjectName functionIdentifier)
        {
            var sql =
                "SELECT specific_schema, routine_name FROM information_schema.routines WHERE type_udt_name != 'trigger' and routine_name like :name and specific_schema = :schema;";

            using var reader = await conn.CreateCommand(sql)
                .With("name", functionIdentifier.Name)
                .With("schema", functionIdentifier.Schema)
                .ExecuteReaderAsync();

            return await reader.ReadAsync();
        }

        public static async Task<IReadOnlyList<DbObjectName>> ExistingTables(this SqlConnection conn,
            string namePattern = null, string[] schemas = null)
        {
            var builder = new CommandBuilder();
            builder.Append("SELECT schemaname, relname FROM pg_stat_user_tables");


            if (namePattern.IsNotEmpty())
            {
                builder.Append(" WHERE relname like :table");
                builder.AddNamedParameter("table", namePattern);

                if (schemas != null)
                {
                    builder.Append(" and schemaname = ANY(:schemas)");
                    builder.AddNamedParameter("schemas", schemas);
                }
            }

            if (schemas != null)
            {
                builder.Append(" WHERE schemaname = ANY(:schemas)");
                builder.AddNamedParameter("schemas", schemas);
            }

            builder.Append(";");

            return await builder.FetchList(conn, ReadDbObjectName);
        }

        public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctions(this SqlConnection conn,
            string namePattern = null, string[] schemas = null)
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

            return await builder.FetchList(conn, ReadDbObjectName);
        }

        private static async Task<DbObjectName> ReadDbObjectName(DbDataReader reader)
        {
            return new(await reader.GetFieldValueAsync<string>(0), await reader.GetFieldValueAsync<string>(1));
        }

        /// <summary>
        ///     Write the creation SQL for this ISchemaObject
        /// </summary>
        /// <param name="object"></param>
        /// <param name="rules"></param>
        /// <returns></returns>
        public static string ToCreateSql(this ISchemaObject @object, DdlRules rules)
        {
            var writer = new StringWriter();
            @object.WriteCreateStatement(rules, writer);

            return writer.ToString();
        }
    }
}