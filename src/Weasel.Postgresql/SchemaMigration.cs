using System.Collections.Generic;
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

        private SchemaMigration(List<ISchemaObjectDelta> deltas)
        {
            _deltas = deltas;
            if (_deltas.Any())
            {
                Difference = _deltas.Min(x => x.Difference);
            }
        }

        public IReadOnlyList<ISchemaObjectDelta> Deltas => _deltas;

        public SchemaPatchDifference Difference { get; private set; } = SchemaPatchDifference.None;
    }
}