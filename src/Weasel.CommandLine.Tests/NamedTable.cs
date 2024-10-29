using JasperFx;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tests;

namespace Weasel.CommandLine.Tests;

public class NamedTable: Table
{
    public NamedTable(DbObjectName name) : base(name)
    {
        AddColumn<Guid>("id").AsPrimaryKey();
        AddColumn<string>("name");
    }
}

public class NamedTableFeature: FeatureSchemaBase
{
    public Dictionary<string, Table> Tables { get; } = new();

    public NamedTableFeature(string identifier, Migrator migrator) : base(identifier, migrator)
    {
    }

    public void AddTable(string schemaName, string tableName)
    {
        var table = new NamedTable(new DbObjectName(schemaName, tableName));
        Tables[table.Identifier.QualifiedName] = table;
    }

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        return Tables.Values;
    }
}

public class DatabaseWithTables: PostgresqlDatabase
{
    public DatabaseWithTables(AutoCreate autoCreate, string identifier) :
        base(new DefaultMigrationLogger(), autoCreate, new PostgresqlMigrator(), identifier, new NpgsqlDataSourceBuilder(ConnectionSource.ConnectionString).Build())
    {
    }

    public LightweightCache<string, NamedTableFeature> Features { get; } =
        new(name =>
            new NamedTableFeature(name, new PostgresqlMigrator()));

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return Features.OfType<IFeatureSchema>().ToArray();
    }
}
