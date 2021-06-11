using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
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
            return conn.CreateCommand("select name from sys.schemas order by name")
                .FetchList<string>();
        }


        public static async Task DropSchema(this SqlConnection conn, string schemaName)
        {
            var procedures = await conn
                .CreateCommand($"select routine_name from information_schema.routines where routine_schema = '{schemaName}';")
                .FetchList<string>();

            var constraints = await conn.CreateCommand($@"
select 
  sys.tables.name as table_name,
  sys.foreign_keys.name as constraint_name
from
  sys.foreign_keys 
      inner join sys.tables on sys.foreign_keys.parent_object_id = sys.tables.object_id
      inner join sys.schemas on sys.tables.schema_id = sys.schemas.schema_id
where
  sys.schemas.name = 'tables';

").FetchList<string>(async r =>
            {
                var tableName = await r.GetFieldValueAsync<string>(0);
                var constraintName = await r.GetFieldValueAsync<string>(1);

                return $"alter table {schemaName}.{tableName} drop constraint {constraintName};";
            });

            var tables = await conn.CreateCommand($@"
   select
       sys.tables.name as table_name
   from
       sys.tables inner join sys.schemas on sys.tables.schema_id = sys.schemas.schema_id
   where
       sys.schemas.name = '{schemaName}';
").FetchList<string>();
            
            var sequences = await conn
                .CreateCommand($@"
   select
       sys.sequences.name
   from
       sys.sequences inner join sys.schemas on sys.sequences.schema_id = sys.schemas.schema_id
   where
       sys.schemas.name = '{schemaName}';

")
                .FetchList<string>();

            var drops = new List<string>();
            drops.AddRange(procedures.Select(name => $"drop procedure {schemaName}.{name};"));
            drops.AddRange(constraints);
            drops.AddRange(tables.Select(name => $"drop table {schemaName}.{name};"));
            drops.AddRange(sequences.Select(name => $"drop sequence {schemaName}.{name};"));
                    
                    
                //     <string>(async r =>
                // {
                //     var name = await r.GetFieldValueAsync<string>(0);
                //     var type = await r.GetFieldValueAsync<string>(1);
                //     switch (type.Trim())
                //     {
                //         case "P":
                //             return ;
                //         case "V":
                //             return $"drop view {schemaName}.{name};";
                //         case "F":
                //             return $"drop constraint {schemaName}.{name};";
                //         case "SO":
                //             return $"drop sequence {schemaName}.{name};";
                //         case "U":
                //             return $"drop table {schemaName}.{name};";
                //         default:
                //             throw new NotSupportedException($"Don't understand type '{type}' for object named '{name}'");
                //     }
                // });

            foreach (var drop in drops)
            {
                await conn.CreateCommand(drop).ExecuteNonQueryAsync();
            } 
            
            if (!schemaName.EqualsIgnoreCase(SqlServerProvider.Instance.DefaultDatabaseSchemaName))
            {
                var sql = $"drop schema if exists {schemaName};";
                await conn.CreateCommand(sql).ExecuteNonQueryAsync();
            }

        }

        public static string DropStatementFor(string schemaName)
        {
            var sql = $@"
/* Drop all non-system stored procs */
DECLARE @name VARCHAR(128)
DECLARE @SQL VARCHAR(254)

SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'P' AND category = 0 ORDER BY [name])

WHILE @name is not null
BEGIN
    SELECT @SQL = 'DROP PROCEDURE [{schemaName}].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Procedure: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'P' AND category = 0 AND [name] > @name ORDER BY [name])
END
GO

/* Drop all views */
DECLARE @name VARCHAR(128)
DECLARE @SQL VARCHAR(254)

SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP VIEW [{schemaName}].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped View: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'V' AND category = 0 AND [name] > @name ORDER BY [name])
END
GO

/* Drop all functions */
DECLARE @name VARCHAR(128)
DECLARE @SQL VARCHAR(254)

SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND category = 0 ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP FUNCTION [{schemaName}].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Function: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] IN (N'FN', N'IF', N'TF', N'FS', N'FT') AND category = 0 AND [name] > @name ORDER BY [name])
END
GO

/* Drop all Foreign Key constraints */
DECLARE @name VARCHAR(128)
DECLARE @constraint VARCHAR(254)
DECLARE @SQL VARCHAR(254)

SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY TABLE_NAME)

WHILE @name is not null
BEGIN
    SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
    WHILE @constraint IS NOT NULL
    BEGIN
        SELECT @SQL = 'ALTER TABLE [{schemaName}].[' + RTRIM(@name) +'] DROP CONSTRAINT [' + RTRIM(@constraint) +']'
        EXEC (@SQL)
        PRINT 'Dropped FK Constraint: ' + @constraint + ' on ' + @name
        SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME <> @constraint AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
    END
SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY TABLE_NAME)
END
GO

/* Drop all Primary Key constraints */
DECLARE @name VARCHAR(128)
DECLARE @constraint VARCHAR(254)
DECLARE @SQL VARCHAR(254)

SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_NAME)

WHILE @name IS NOT NULL
BEGIN
    SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
    WHILE @constraint is not null
    BEGIN
        SELECT @SQL = 'ALTER TABLE [{schemaName}].[' + RTRIM(@name) +'] DROP CONSTRAINT [' + RTRIM(@constraint)+']'
        EXEC (@SQL)
        PRINT 'Dropped PK Constraint: ' + @constraint + ' on ' + @name
        SELECT @constraint = (SELECT TOP 1 CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' AND CONSTRAINT_NAME <> @constraint AND TABLE_NAME = @name ORDER BY CONSTRAINT_NAME)
    END
SELECT @name = (SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE constraint_catalog=DB_NAME() AND CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_NAME)
END
GO

/* Drop all tables */
DECLARE @name VARCHAR(128)
DECLARE @SQL VARCHAR(254)

SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 ORDER BY [name])

WHILE @name IS NOT NULL
BEGIN
    SELECT @SQL = 'DROP TABLE [{schemaName}].[' + RTRIM(@name) +']'
    EXEC (@SQL)
    PRINT 'Dropped Table: ' + @name
    SELECT @name = (SELECT TOP 1 [name] FROM sysobjects WHERE [type] = 'U' AND category = 0 AND [name] > @name ORDER BY [name])
END
GO
";

            if (!schemaName.EqualsIgnoreCase(SqlServerProvider.Instance.DefaultDatabaseSchemaName))
            {
                sql += $"{Environment.NewLine}drop schema if exists {schemaName};{Environment.NewLine}GO;";
            }

            return sql;
        }

        public static Task CreateSchema(this SqlConnection conn, string schemaName)
        {
            return conn.CreateCommand(SchemaMigration.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync();
        }

        public static async Task ResetSchema(this SqlConnection conn, string schemaName)
        {
            await conn.DropSchema(schemaName);
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