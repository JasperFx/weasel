using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Weasel.Core;

namespace Weasel.SqlServer
{
    public class Sequence : ISchemaObject
    {
        private readonly long? _startWith;

        public Sequence(string identifier)
        {
            Identifier = DbObjectName.Parse(SqlServerProvider.Instance, identifier);
        }

        public Sequence(DbObjectName identifier)
        {
            Identifier = identifier;
        }

        public Sequence(DbObjectName identifier, long startWith)
        {
            Identifier = identifier;
            _startWith = startWith;
        }

        public DbObjectName Owner { get; set; }
        public string OwnerColumn { get; set; }
        public DbObjectName Identifier { get; }

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }

        public void WriteCreateStatement(DdlRules rules, TextWriter writer)
        {
            var startsWith = _startWith.HasValue ? _startWith.Value : 1;
            
            writer.WriteLine(
                $"CREATE SEQUENCE {Identifier} START WITH {startsWith};");

            if (Owner != null)
            {
                writer.WriteLine($"ALTER SEQUENCE {Identifier} OWNED BY {Owner}.{OwnerColumn};");
            }
        }

        public void WriteDropStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine($"DROP SEQUENCE IF EXISTS {Identifier};");
        }

        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
            var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
            builder.Append($"select count(*) from sys.sequences inner join sys.schemas on sys.sequences.schema_id = sys.schemas.schema_id where sys.schemas.name = @{schemaParam} and sys.sequences.name = @{nameParam};");

        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            if (!await reader.ReadAsync() || await reader.GetFieldValueAsync<int>(0) == 0)
            {
                return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
            }

            return new SchemaObjectDelta(this, SchemaPatchDifference.None);
        }

        public async Task<ISchemaObjectDelta> FindDelta(SqlConnection conn)
        {
            var builder = new CommandBuilder();

            ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);

            return await CreateDelta(reader);
        }
    }
}