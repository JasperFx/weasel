using System.Data.Common;
using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using MySqlConnector;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.MySql;

public class DatabaseWithTables: DatabaseBase<MySqlConnection>, IDatabaseWithTables
{
    private readonly List<ITable> _tables = new();
    private readonly string _connectionString;

    public DatabaseWithTables(string identifier, string connectionString)
        : base(new DefaultMigrationLogger(), AutoCreate.All, new MySqlMigrator(), identifier, connectionString)
    {
        _connectionString = connectionString;
    }

    public DatabaseWithTables(string identifier, string connectionString, AutoCreate autoCreate)
        : base(new DefaultMigrationLogger(), autoCreate, new MySqlMigrator(), identifier, connectionString)
    {
        _connectionString = connectionString;
    }

    public IReadOnlyList<ITable> Tables => _tables;

    public ITable AddTable(DbObjectName identifier)
    {
        var table = Migrator.CreateTable(identifier);
        _tables.Add(table);
        return table;
    }

    public void AddTable(ITable table)
    {
        _tables.Add(table);
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
        => [new TableFeatureSchema(Migrator, _tables)];

    public override DatabaseDescriptor Describe()
    {
        var builder = new MySqlConnectionStringBuilder(_connectionString);
        return new DatabaseDescriptor
        {
            Engine = MySqlProvider.EngineName,
            ServerName = builder.Server ?? string.Empty,
            DatabaseName = builder.Database ?? string.Empty,
            Subject = GetType().FullNameInCode(),
            Identifier = Identifier
        };
    }

    private class TableFeatureSchema(Migrator migrator, List<ITable> tables)
        : FeatureSchemaBase("Tables", migrator)
    {
        public override Type StorageType => typeof(DatabaseWithTables);
        protected override IEnumerable<ISchemaObject> schemaObjects() => tables;
    }
}
