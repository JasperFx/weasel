using System.Data.Common;
using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer;

public class DatabaseWithTables: DatabaseBase<SqlConnection>
{
    private readonly List<ITable> _tables = new();
    private readonly string _connectionString;

    public DatabaseWithTables(string identifier, string connectionString)
        : base(new DefaultMigrationLogger(), AutoCreate.All, new SqlServerMigrator(), identifier, connectionString)
    {
        _connectionString = connectionString;
    }

    public DatabaseWithTables(string identifier, string connectionString, AutoCreate autoCreate)
        : base(new DefaultMigrationLogger(), autoCreate, new SqlServerMigrator(), identifier, connectionString)
    {
        _connectionString = connectionString;
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

    public override DatabaseDescriptor Describe()
    {
        var builder = new SqlConnectionStringBuilder(_connectionString);
        return new DatabaseDescriptor
        {
            Engine = SqlServerProvider.EngineName,
            ServerName = builder.DataSource ?? string.Empty,
            DatabaseName = builder.InitialCatalog ?? string.Empty,
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
