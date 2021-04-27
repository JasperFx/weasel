using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Weasel.Postgresql
{
    public class Sequence: ISchemaObject
    {
        public DbObjectName Identifier { get; }

        private readonly long? _startWith;

        public Sequence(string identifier)
        {
            Identifier = DbObjectName.Parse(identifier);
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

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }

        public void WriteCreateStatement(DdlRules rules, StringWriter writer)
        {
            writer.WriteLine($"CREATE SEQUENCE {Identifier}{(_startWith.HasValue ? $" START {_startWith.Value}" : string.Empty)};");

            if (Owner != null)
            {
                writer.WriteLine($"ALTER SEQUENCE {Identifier} OWNED BY {Owner}.{OwnerColumn};");
            }
        }

        public void WriteDropStatement(DdlRules rules, StringWriter writer)
        {
            writer.WriteLine($"DROP SEQUENCE IF EXISTS {Identifier};");
        }

        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
            var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
            builder.Append($"select count(*) from information_schema.sequences where sequence_schema = :{schemaParam} and sequence_name = :{nameParam};");
        }

        public async Task<SchemaPatchDifference> CreatePatch(DbDataReader reader, SchemaPatch patch, AutoCreate autoCreate)
        {
            if (!await reader.ReadAsync() || (await reader.GetFieldValueAsync<int>(0)) == 0)
            {
                WriteCreateStatement(patch.Rules, patch.UpWriter);
                return SchemaPatchDifference.Create;
            }

            return SchemaPatchDifference.None;
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            if (!await reader.ReadAsync() || (await reader.GetFieldValueAsync<int>(0)) == 0)
            {
                return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
            }

            return new SchemaObjectDelta(this, SchemaPatchDifference.None);
        }
    }
}
