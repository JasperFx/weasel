using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Baseline;

namespace Weasel.SqlServer
{
    public class SchemaMigration
    {
        private readonly List<ISchemaObjectDelta> _deltas;
        private readonly string[] _schemas;
        private readonly Lazy<string> _rollbacks;
        private readonly Lazy<string> _updates;

        public SchemaMigration(IEnumerable<ISchemaObjectDelta> deltas)
        {
            _deltas = new List<ISchemaObjectDelta>(deltas);
            _schemas = _deltas.SelectMany(x => x.SchemaObject.AllNames())
                .Select(x => x.Schema)
                .Where(x => x != "public")
                .Distinct().ToArray();

            if (_deltas.Any())
            {
                Difference = _deltas.Min(x => x.Difference);
            }

            _updates = new Lazy<string>(() =>
            {
                var writer = new StringWriter();
                WriteAllUpdates(writer, new DdlRules(), AutoCreate.CreateOrUpdate);

                return writer.ToString();
            });

            _rollbacks = new Lazy<string>(() =>
            {
                var writer = new StringWriter();
                WriteAllRollbacks(writer, new DdlRules());

                return writer.ToString();
            });
        }

        public SchemaMigration(ISchemaObjectDelta delta) : this(new[] {delta})
        {
        }

        public IReadOnlyList<ISchemaObjectDelta> Deltas => _deltas;

        public SchemaPatchDifference Difference { get; } = SchemaPatchDifference.None;

        /// <summary>
        ///     The SQL that will be executed to update this migration
        /// </summary>
        public string UpdateSql => _updates.Value;

        /// <summary>
        ///     The SQL to rollback the application of this migration
        /// </summary>
        public string RollbackSql => _rollbacks.Value;

        public static async Task<SchemaMigration> Determine(SqlConnection conn, params ISchemaObject[] schemaObjects)
        {
            var deltas = new List<ISchemaObjectDelta>();


            if (!schemaObjects.Any())
            {
                return new SchemaMigration(deltas);
            }

            var builder = new CommandBuilder();

            foreach (var schemaObject in schemaObjects) schemaObject.ConfigureQueryCommand(builder);

            using var reader = await builder.ExecuteReaderAsync(conn);

            deltas.Add(await schemaObjects[0].CreateDelta(reader));

            for (var i = 1; i < schemaObjects.Length; i++)
            {
                await reader.NextResultAsync();
                deltas.Add(await schemaObjects[i].CreateDelta(reader));
            }

            return new SchemaMigration(deltas);
        }

        public async Task ApplyAll(SqlConnection conn, DdlRules rules, AutoCreate autoCreate,
            Action<string>? logSql = null, Action<SqlCommand, Exception>? onFailure = null)
        {
            if (autoCreate == AutoCreate.None)
            {
                return;
            }

            if (Difference == SchemaPatchDifference.None)
            {
                return;
            }

            if (!_deltas.Any())
            {
                return;
            }

            await createSchemas(conn, logSql, onFailure);


            AssertPatchingIsValid(autoCreate);
            
            foreach (var delta in _deltas)
            {
                var writer = new StringWriter();
                writeUpdate(writer, rules, delta);

                if (writer.ToString().Trim().IsNotEmpty())
                {
                    await executeCommand(conn, logSql, onFailure, writer);
                }
            }

        }

        private async Task createSchemas(SqlConnection conn, Action<string>? logSql, Action<SqlCommand, Exception>? onFailure)
        {
            var writer = new StringWriter();

            if (_schemas.Any())
            {
                SchemaGenerator.WriteSql(_schemas, writer);
                if (writer.ToString().Trim().IsNotEmpty()) // Cheesy way of knowing if there is any delta
                {
                    await executeCommand(conn, logSql, onFailure, writer);
                }
            }
        }

        private static async Task executeCommand(SqlConnection conn, Action<string>? logSql, Action<SqlCommand, Exception>? onFailure, StringWriter writer)
        {
            var cmd = conn.CreateCommand(writer.ToString());
            logSql?.Invoke(cmd.CommandText);

            try
            {
                await cmd
                    .ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                if (onFailure != null)
                {
                    onFailure(cmd, e);
                }
                else
                {
                    throw;
                }
            }
        }

        public void WriteAllUpdates(TextWriter writer, DdlRules rules, AutoCreate autoCreate)
        {
            AssertPatchingIsValid(autoCreate);
            foreach (var delta in _deltas)
            {
                writeUpdate(writer, rules, delta);
            }
        }

        private static bool writeUpdate(TextWriter writer, DdlRules rules, ISchemaObjectDelta delta)
        {
            switch (delta.Difference)
            {
                case SchemaPatchDifference.None:
                    return false;

                case SchemaPatchDifference.Create:
                    delta.SchemaObject.WriteCreateStatement(rules, writer);
                    return true;

                case SchemaPatchDifference.Update:
                    delta.WriteUpdate(rules, writer);
                    return true;

                case SchemaPatchDifference.Invalid:
                    delta.SchemaObject.WriteDropStatement(rules, writer);
                    delta.SchemaObject.WriteCreateStatement(rules, writer);
                    return true;
            }

            return false;
        }

        public void WriteAllRollbacks(TextWriter writer, DdlRules rules)
        {
            foreach (var delta in _deltas)
            {
                switch (delta.Difference)
                {
                    case SchemaPatchDifference.None:
                        continue;

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
            if (Difference == SchemaPatchDifference.None)
            {
                return;
            }

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


        public Task RollbackAll(SqlConnection conn, DdlRules rules)
        {
            var writer = new StringWriter();
            WriteAllRollbacks(writer, rules);

            return conn
                .CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }

        public static string CreateSchemaStatementFor(string schemaName)
        {
            return $@"
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{schemaName}' )
    EXEC('CREATE SCHEMA [{schemaName}]');

";
        }
    }
}