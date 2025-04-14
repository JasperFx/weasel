using System.Data.Common;
using JasperFx.Core;
using Weasel.Core;
using Weasel.Core.Migrations;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql;

public class SchemaExistenceCheck : ISchemaObject
{
    public static ISchemaObject[] WithSchemaCheck(ISchemaObject[] others)
    {
        var check = new SchemaExistenceCheck(others);
        return new ISchemaObject[] { check }.Concat(others).ToArray();
    }

    private readonly string[] _schemas;
    private string[] _missing;

    public SchemaExistenceCheck(ISchemaObject[] others)
    {
        _schemas = others.SelectMany(x => x.AllNames()).Select(x => x.Schema).Distinct().ToArray();
        Identifier = new DbObjectName("Database", "Schemas");
    }

    public DbObjectName Identifier { get; }
    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (_missing != null && _missing.Any())
        {
            migrator.WriteSchemaCreationSql(_missing, writer);
        }
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // Nothing
    }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        builder.Append("SELECT schema_name FROM information_schema.schemata;");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var schemas = new List<string>();
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            schemas.Add(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false));
        }

        var missing = _schemas.Where(x => !schemas.Contains(x)).ToArray();
        _missing = missing;

        return new MissingSchemaDelta(this, missing);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield break;
    }

    internal class MissingSchemaDelta: ISchemaObjectDelta
    {
        private readonly string[] _missing;

        public MissingSchemaDelta(SchemaExistenceCheck schemaExistenceCheck, string[] missing)
        {
            _missing = missing;
            SchemaObject = schemaExistenceCheck;

            Difference = missing.Any() ? SchemaPatchDifference.Create : SchemaPatchDifference.None;
        }

        public ISchemaObject SchemaObject { get; }
        public SchemaPatchDifference Difference { get; }
        public void WriteUpdate(Migrator rules, TextWriter writer)
        {
            rules.WriteSchemaCreationSql(_missing, writer);
        }

        public void WriteRollback(Migrator rules, TextWriter writer)
        {
            foreach (var schemaName in _missing)
            {
                writer.WriteLine($"drop schema if exists {schemaName};");
            }
        }

        public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
        {
            // Nothing
        }
    }
}
