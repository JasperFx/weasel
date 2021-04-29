using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Npgsql;
using Weasel.Postgresql.Functions;

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

        public SchemaMigration(ISchemaObjectDelta delta)
        {
            _deltas = new List<ISchemaObjectDelta> {delta};
            Difference = delta.Difference;
        }

        public IReadOnlyList<ISchemaObjectDelta> Deltas => _deltas;

        public SchemaPatchDifference Difference { get; private set; } = SchemaPatchDifference.None;
        
        public Task ApplyAll(NpgsqlConnection conn, DdlRules rules, AutoCreate autoCreate)
        {
            if (autoCreate == AutoCreate.None) return Task.CompletedTask;
            if (Difference == SchemaPatchDifference.None) return Task.CompletedTask;
            if (!_deltas.Any()) return Task.CompletedTask;

            var writer = new StringWriter();
            
            WriteAllUpdates(writer, rules, autoCreate);
            
            return conn.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }

        public void WriteAllUpdates(TextWriter writer, DdlRules rules, AutoCreate autoCreate)
        {
            AssertPatchingIsValid(autoCreate);
            foreach (var delta in _deltas)
            {
                switch (delta.Difference)
                {
                    case SchemaPatchDifference.None:
                        return;
                    
                    case SchemaPatchDifference.Create:
                        delta.SchemaObject.WriteCreateStatement(rules, writer);
                        break;
                    
                    case SchemaPatchDifference.Update:
                        delta.WriteUpdate(rules, writer);
                        break;
                    
                    case SchemaPatchDifference.Invalid:
                        delta.SchemaObject.WriteDropStatement(rules, writer);
                        delta.SchemaObject.WriteCreateStatement(rules, writer);
                        break;
                }
            }
        }

        public void WriteAllRollbacks(TextWriter writer, DdlRules rules)
        {
            foreach (var delta in _deltas)
            {
                switch (delta.Difference)
                {
                    case SchemaPatchDifference.None:
                        return;
                    
                    case SchemaPatchDifference.Create:
                        delta.SchemaObject.WriteDropStatement(rules, writer);
                        break;
                    
                    case SchemaPatchDifference.Update:
                        delta.WriteRollback(rules, writer);
                        break;
                    
                    case SchemaPatchDifference.Invalid:
                        delta.SchemaObject.WriteDropStatement(rules, writer);
                        delta.WriteRestorationOfPreviousState(rules, writer);
                        break;
                }
            }
        }
        
        public static string ToDropFileName(string updateFile)
        {
            var containingFolder = updateFile.ParentDirectory();
            var rawFileName = Path.GetFileNameWithoutExtension(updateFile);
            var ext = Path.GetExtension(updateFile);

            var dropFile = $"{rawFileName}.drop{ext}";

            return containingFolder.IsEmpty() ? dropFile : containingFolder.AppendPath(dropFile);
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


        public Task RollbackAll(NpgsqlConnection conn, DdlRules rules)
        {
            var writer = new StringWriter();
            WriteAllRollbacks(writer, rules);

            return conn
                .CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }
    }
}