using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using Weasel.Core.Migrations;

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

        return descriptor;
    }
}
