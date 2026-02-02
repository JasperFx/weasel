using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using Weasel.Core.Migrations;
using Table = Weasel.Sqlite.Tables.Table;

namespace Weasel.Sqlite;

public abstract class SqliteDatabase: DatabaseBase<SqliteConnection>
{
    protected SqliteDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        string connectionString
    ): base(logger, autoCreate, migrator, identifier, () => new SqliteConnection(connectionString))
    {
        ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));

        // ReSharper disable once VirtualMemberCallInConstructor
        var descriptor = Describe();
        Id = new DatabaseId(descriptor.ServerName, descriptor.DatabaseName);
    }

    public string ConnectionString { get; }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Table(identifier);
    }

    public override DatabaseDescriptor Describe()
    {
        var builder = new SqliteConnectionStringBuilder(ConnectionString);
        var descriptor = new DatabaseDescriptor()
        {
            Engine = SqliteProvider.EngineName,
            ServerName = "localhost", // SQLite is file-based
            DatabaseName = builder.DataSource ?? ":memory:",
            Subject = GetType().FullNameInCode(),
            Identifier = Identifier
        };

        descriptor.TenantIds.AddRange(TenantIds);

        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.DataSource));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Mode));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Cache));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Pooling));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.RecursiveTriggers));

        descriptor.Properties.RemoveAll(x => x.Name.ContainsIgnoreCase("password"));

        return descriptor;
    }
}
