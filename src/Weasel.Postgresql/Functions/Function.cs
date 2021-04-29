using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Npgsql;

namespace Weasel.Postgresql.Functions
{
    public class Function : ISchemaObject
    {
        /* TODO
         * load from a file?
         * load from an embedded resource
         * parse drop statements
         *
         *
         * 
         * 
         */


        private readonly string _body;
        private readonly string[] _dropStatements;

        public static string ParseSignature(string body)
        {
            var functionIndex = body.IndexOf("FUNCTION", StringComparison.OrdinalIgnoreCase);
            var openParen = body.IndexOf("(");
            var closeParen = body.IndexOf(")");

            var args = body.Substring(openParen + 1, closeParen - openParen - 1).Trim()
                .Split(',').Select(x =>
                {
                    var parts = x.Trim().Split(' ');
                    return parts.Skip(1).Join(" ");
                }).Join(", ");

            var nameStart = functionIndex + "function".Length;
            var funcName = body.Substring(nameStart, openParen - nameStart).Trim();

            return $"{funcName}({args})";
        }
        
        public static DbObjectName ParseIdentifier(string functionSql)
        {
            var signature = ParseSignature(functionSql);
            var open = signature.IndexOf('(');
            return DbObjectName.Parse(signature.Substring(0, open));
        }


        public Function(DbObjectName identifier, string body, string[] dropStatements)
        {
            _body = body;
            _dropStatements = dropStatements;
            Identifier = identifier;
        }

        public Function(DbObjectName identifier, string body)
        {
            _body = body;
            Identifier = identifier;
        }


        public virtual void WriteCreateStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine(_body);
        }

        public void WriteDropStatement(DdlRules rules, TextWriter writer)
        {
            foreach (var dropStatement in _dropStatements)
            {
                writer.WriteLine(dropStatement);
            }
        }

        public DbObjectName Identifier { get; }
        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
            var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

            builder.Append($@"
SELECT pg_get_functiondef(pg_proc.oid)
FROM pg_proc JOIN pg_namespace as ns ON pg_proc.pronamespace = ns.oid WHERE ns.nspname = :{schemaParam} and proname = :{nameParam};

SELECT format('DROP FUNCTION IF EXISTS %s.%s(%s);'
             ,n.nspname
             ,p.proname
             ,pg_get_function_identity_arguments(p.oid))
FROM   pg_proc p
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE  p.proname = :{nameParam}
AND    n.nspname = :{schemaParam};
");
        }


        public Task<SchemaPatchDifference> CreatePatch(DbDataReader reader, SchemaPatch patch, AutoCreate autoCreate)
        {
            throw new NotImplementedException();
        }
        
        public bool IsRemoved { get; private set; }

        public async Task<Function> FetchExisting(NpgsqlConnection conn)
        {
            var builder = new CommandBuilder();

            ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);
            return await readExisting(reader);
        }
        
        private async Task<Function> readExisting(DbDataReader reader)
        {
            if (!await reader.ReadAsync())
            {
                await reader.NextResultAsync();
                return null;
            }

            var existingFunction = await reader.GetFieldValueAsync<string>(0);

            if (string.IsNullOrEmpty(existingFunction))
            {
                return null;
            }

            await reader.NextResultAsync();
            var drops = new List<string>();
            while (await reader.ReadAsync())
            {
                drops.Add(await reader.GetFieldValueAsync<string>(0));
            }

            return new Function(Identifier, existingFunction.TrimEnd() + ";", drops.ToArray());
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader);
            return new FunctionDelta(this, existing);
        }

        public IEnumerable<DbObjectName> AllNames()
        {
            throw new NotImplementedException();
        }

        public string Body()
        {
            var writer = new StringWriter();
            WriteCreateStatement(new DdlRules(), writer);

            return writer.ToString();
        }

        public string[] DropStatements()
        {
            if (_dropStatements?.Length > 0) return _dropStatements;

            var signature = ParseSignature(Body());

            var drop = $"drop function {signature};";

            return new [] {drop};
        }


        public static Function ForSql(string sql)
        {
            var identifier = ParseIdentifier(sql);
            return new Function(identifier, sql);
        }

        public async Task<FunctionDelta> FindDelta(NpgsqlConnection conn)
        {
            var existing = await FetchExisting(conn);
            return new FunctionDelta(this, existing);
        }

        public static Function ForRemoval(string identifier)
        {
            return ForRemoval(DbObjectName.Parse(identifier));
        }
        
        public static Function ForRemoval(DbObjectName identifier)
        {
            return new Function(identifier, null)
            {
                IsRemoved = true
            };
        }
    }
}