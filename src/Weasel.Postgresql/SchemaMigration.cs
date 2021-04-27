using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql
{
    public class SchemaMigration
    {
        private readonly List<ISchemaObjectDelta> _deltas;
        
        public static async Task<SchemaMigration> Determine(NpgsqlConnection conn, ISchemaObject[] schemaObjects)
        {
            var deltas = new List<ISchemaObjectDelta>();
            
            if (!schemaObjects.Any())
            {
                return new SchemaMigration(deltas);
            }
            
            var builder = new CommandBuilder();

            foreach (var schemaObject in schemaObjects)
            {
                schemaObject.ConfigureQueryCommand(builder);
            }

            using var reader = await builder.ExecuteReaderAsync(conn);
            
            deltas.Add(await schemaObjects[0].CreateDelta(reader));
            
            for (var i = 1; i < schemaObjects.Length; i++)
            {
                await reader.NextResultAsync();
                deltas.Add(await schemaObjects[i].CreateDelta(reader));
            }

            return new SchemaMigration(deltas);
        }

        public SchemaMigration(IEnumerable<ISchemaObjectDelta> deltas)
        {
            _deltas = new List<ISchemaObjectDelta>(deltas);
            if (_deltas.Any())
            {
                Difference = _deltas.Min(x => x.Difference);
            }
        }

        public IReadOnlyList<ISchemaObjectDelta> Deltas => _deltas;

        public SchemaPatchDifference Difference { get; private set; } = SchemaPatchDifference.None;


        public Task ApplyAllUpdates(NpgsqlConnection conn, DdlRules rules)
        {
            if (Difference == SchemaPatchDifference.None) return Task.CompletedTask;
            if (!_deltas.Any()) return Task.CompletedTask;
            
            // TODO -- if any deltas are invalid, throw
            
            var writer = new StringWriter();
            foreach (var delta in _deltas)
            {
                delta.WriteUpdate(rules, writer);
            }

            return conn.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }
        
        public void AssertPatchingIsValid(AutoCreate autoCreate)
        {
            if (Difference == SchemaPatchDifference.None) return;
            
            switch (autoCreate)
            {
                case AutoCreate.All:
                case AutoCreate.None:
                    return;
                
                case AutoCreate.CreateOnly:
                    if (Difference != SchemaPatchDifference.Create)
                    {
                        var invalids = _deltas.Where(x => x.Difference < SchemaPatchDifference.Create);
                        throw new SchemaMigrationException(autoCreate, invalids);
                    }

                    break;
                
                case AutoCreate.CreateOrUpdate:
                    if (Difference == SchemaPatchDifference.Invalid)
                    {
                        var invalids = _deltas.Where(x => x.Difference == SchemaPatchDifference.Invalid);
                        throw new SchemaMigrationException(autoCreate, invalids);
                    }

                    break;
            }

        }
    }
}