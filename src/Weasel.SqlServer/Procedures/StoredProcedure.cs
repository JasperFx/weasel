using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Baseline;
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

        public Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            throw new System.NotImplementedException();
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

            return body.Trim();
        }
    }
}
