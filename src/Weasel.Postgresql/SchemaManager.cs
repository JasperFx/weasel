using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql
{
    public enum CascadeOption
    {
        Cascade,
        Restrict
    }
    
    public static class SchemaManager
    {
        public static async Task EnsureSchemaExists(NpgsqlConnection conn, string schemaName, CancellationToken cancellation = default)
        {
            bool shouldClose = false;
            if (conn.State != ConnectionState.Open)
            {
                shouldClose = true;
                await conn.OpenAsync(cancellation);
            }

            try
            {
                await conn
                    .CreateCommand(CreateStatementFor(schemaName))
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

        public static Task<IReadOnlyList<string>> ActiveSchemaNames(NpgsqlConnection conn)
        {
            return conn.CreateCommand("select nspname from pg_catalog.pg_namespace order by nspname")
                .FetchList<string>();
        }


        public static Task DropSchema(this NpgsqlConnection conn, string schemaName)
        {
            return conn.CreateCommand(DropStatementFor(schemaName)).ExecuteNonQueryAsync();
        }
        
        public static string DropStatementFor(string schemaName, CascadeOption option = CascadeOption.Cascade)
        {
            return $"drop schema if exists {schemaName} {option.ToString().ToUpperInvariant()};";
        }

        public static Task CreateSchema(this NpgsqlConnection conn, string schemaName)
        {
            return conn.CreateCommand(CreateStatementFor(schemaName)).ExecuteNonQueryAsync();
        }

        public static string CreateStatementFor(string schemaName)
        {
            return $"create schema if not exists {schemaName};";
        }

        public static Task ResetSchema(this NpgsqlConnection conn, string schemaName)
        {
            return conn.RunSql(DropStatementFor(schemaName, CascadeOption.Cascade), CreateStatementFor(schemaName));
        }
    }
}