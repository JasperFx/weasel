using JasperFx;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Postgresql;

public class DatabaseWithTables: PostgresqlDatabase
{
    private readonly List<ITable> _tables = new();

    public DatabaseWithTables(string identifier, NpgsqlDataSource dataSource)
        : base(new DefaultMigrationLogger(), AutoCreate.All, new PostgresqlMigrator(), identifier, dataSource)
    {
    }

    public DatabaseWithTables(string identifier, NpgsqlDataSource dataSource, AutoCreate autoCreate)
        : base(new DefaultMigrationLogger(), autoCreate, new PostgresqlMigrator(), identifier, dataSource)
    {
    }

    public IReadOnlyList<ITable> Tables => _tables;

    public ITable CreateTable(DbObjectName identifier)
    {
        var table = Migrator.CreateTable(identifier);
        _tables.Add(table);
        return table;
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
        => [new TableFeatureSchema(Migrator, _tables)];

    private class TableFeatureSchema(Migrator migrator, List<ITable> tables)
        : FeatureSchemaBase("Tables", migrator)
    {
        public override Type StorageType => typeof(DatabaseWithTables);
        protected override IEnumerable<ISchemaObject> schemaObjects() => tables;
    }
}
