using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Baseline.ImTools;
using Weasel.Core;

namespace Weasel.SqlServer.Procedures
{
    public class StoredProcedure : ISchemaObject
    {
        private readonly string _body;

        public StoredProcedure(DbObjectName identifier)
        {
            Identifier = identifier;
        }

        public StoredProcedure(DbObjectName identifier, string body)
        {
            Identifier = identifier;
            _body = body;
        }

        protected virtual string generateBody(TextWriter writer)
        {
            throw new NotSupportedException(
                "This must be implemented in subclasses that do not inject the procedure body");
        }

        public void WriteCreateStatement(DdlRules rules, TextWriter writer)
        {
            if (_body.IsNotEmpty())
            {
                writer.WriteLine(_body);
            }
            else
            {
                generateBody(writer);
            }
        }
        
        public void WriteCreateOrAlterStatement(DdlRules rules, TextWriter writer)
        {
            var body = _body;
            if (_body.IsEmpty())
            {
                var w = new StringWriter();
                generateBody(w);

                body = w.ToString();
            }

            body = body.Replace("CREATE PROCEDURE", "CREATE OR ALTER PROCEDURE");
            body = body.Replace("create procedure", "create or alter procedure");
            
            writer.WriteLine(body);
        }

        public void WriteDropStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine($"drop procedure if exists {Identifier};");
        }

        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            builder.Append($@"
select
    sys.sql_modules.definition
from sys.sql_modules
inner join sys.objects on sys.sql_modules.object_id = sys.objects.object_id
inner join sys.schemas on sys.objects.schema_id = sys.schemas.schema_id
where
    sys.objects.name = '{Identifier.Name}' and
    sys.schemas.name = '{Identifier.Schema}'
");
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader);
            return new StoredProcedureDelta(this, existing);
        }

        private async Task<StoredProcedure> readExisting(DbDataReader reader)
        {
            if (await reader.ReadAsync())
            {
                var body = await reader.GetFieldValueAsync<string>(0);
                return new StoredProcedure(Identifier, body);
            }

            return null;
        }

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }

        public DbObjectName Identifier { get; }
        public bool IsRemoved { get; set; }

        public string CanonicizeSql()
        {
            var body = _body;
            if (_body.IsEmpty())
            {
                var writer = new StringWriter();
                generateBody(writer);

                body = writer.ToString();
            }

            return body.ReadLines().Select(x => x.Trim()).Where(x => x.IsNotEmpty())
                .Select(x => x.Replace("   ", " ")).Join(Environment.NewLine);
        }
        
        public async Task<StoredProcedure> FetchExisting(SqlConnection conn)
        {
            var builder = new CommandBuilder();

            ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);
            return await readExisting(reader);
        }


        
        public async Task<StoredProcedureDelta> FindDelta(SqlConnection conn)
        {
            var actual = await FetchExisting(conn);
            return new StoredProcedureDelta(this, actual);
        }


    }


}
