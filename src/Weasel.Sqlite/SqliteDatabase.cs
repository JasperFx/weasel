using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Sqlite.Functions;
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
    ): this(logger, autoCreate, migrator, identifier, connectionString, SqlitePragmaSettings.Default)
    {
    }

    protected SqliteDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        string connectionString,
        SqlitePragmaSettings pragmaSettings
    ): this(logger, autoCreate, migrator, identifier, connectionString, pragmaSettings, new SqliteExtensionSettings())
    {
    }

    protected SqliteDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        string connectionString,
        SqlitePragmaSettings pragmaSettings,
        SqliteExtensionSettings extensionSettings
    ): this(logger, autoCreate, migrator, identifier, connectionString, pragmaSettings, extensionSettings, new SqliteFunctionRegistry())
    {
    }

    protected SqliteDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        string connectionString,
        SqlitePragmaSettings pragmaSettings,
        SqliteExtensionSettings extensionSettings,
        SqliteFunctionRegistry functionRegistry
    ): base(logger, autoCreate, migrator, identifier, CreateConnectionFactory(connectionString, pragmaSettings))
    {
        ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        PragmaSettings = pragmaSettings ?? throw new ArgumentNullException(nameof(pragmaSettings));
        ExtensionSettings = extensionSettings ?? throw new ArgumentNullException(nameof(extensionSettings));
        FunctionRegistry = functionRegistry ?? throw new ArgumentNullException(nameof(functionRegistry));

        // ReSharper disable once VirtualMemberCallInConstructor
        var descriptor = Describe();
        Id = new DatabaseId(descriptor.ServerName, descriptor.DatabaseName);
    }

    private static Func<SqliteConnection> CreateConnectionFactory(string connectionString, SqlitePragmaSettings pragmaSettings)
    {
        // Note: The connection factory just creates the connection.
        // PRAGMA settings should be applied after opening via ApplyPragmaSettingsAsync
        return () => new SqliteConnection(connectionString);
    }

    public string ConnectionString { get; }

    /// <summary>
    /// PRAGMA settings applied to new connections.
    /// Call ApplyPragmaSettingsAsync after opening a connection to apply these settings.
    /// </summary>
    public SqlitePragmaSettings PragmaSettings { get; }

    /// <summary>
    /// Extension settings for loading SQLite extensions.
    /// Extensions are loaded when ApplyExtensionsAsync is called after opening a connection.
    /// </summary>
    public SqliteExtensionSettings ExtensionSettings { get; }

    /// <summary>
    /// Function registry for custom scalar and aggregate functions.
    /// Functions are registered when ApplyFunctionsAsync is called after opening a connection.
    /// Note: SQLite functions are connection-scoped and must be re-registered for each connection.
    /// </summary>
    public SqliteFunctionRegistry FunctionRegistry { get; }

    /// <summary>
    /// Apply PRAGMA settings to an open connection.
    /// This should be called after opening a connection created by this database.
    /// </summary>
    public async Task ApplyPragmaSettingsAsync(SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before applying PRAGMA settings");
        }

        await PragmaSettings.ApplyToConnectionAsync(connection, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Apply extension settings to an open connection by loading all configured extensions.
    /// This should be called after opening a connection created by this database.
    /// </summary>
    public async Task ApplyExtensionsAsync(SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before loading extensions");
        }

        await ExtensionSettings.ApplyToConnectionAsync(connection, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Apply custom functions to an open connection by registering all functions in the registry.
    /// This should be called after opening a connection created by this database.
    /// </summary>
    public async Task ApplyFunctionsAsync(SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before registering functions");
        }

        await FunctionRegistry.RegisterAllAsync(connection, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Apply PRAGMA settings, extensions, and functions to an open connection.
    /// This is a convenience method that calls ApplyPragmaSettingsAsync, ApplyExtensionsAsync, and ApplyFunctionsAsync.
    /// </summary>
    public async Task ApplyConnectionSettingsAsync(SqliteConnection connection, CancellationToken ct = default)
    {
        await ApplyPragmaSettingsAsync(connection, ct).ConfigureAwait(false);
        await ApplyExtensionsAsync(connection, ct).ConfigureAwait(false);
        await ApplyFunctionsAsync(connection, ct).ConfigureAwait(false);
    }

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

        // Add PRAGMA settings info
        descriptor.Properties.Add(new OptionsValue("JournalMode", PragmaSettings.JournalMode.ToString(), PragmaSettings.JournalMode));
        descriptor.Properties.Add(new OptionsValue("Synchronous", PragmaSettings.Synchronous.ToString(), PragmaSettings.Synchronous));
        descriptor.Properties.Add(new OptionsValue("CacheSize", PragmaSettings.CacheSize.ToString(), PragmaSettings.CacheSize));
        descriptor.Properties.Add(new OptionsValue("ForeignKeys", PragmaSettings.ForeignKeys.ToString(), PragmaSettings.ForeignKeys));

        // Add extension info
        if (ExtensionSettings.Extensions.Any())
        {
            var extensionNames = string.Join(", ", ExtensionSettings.Extensions.Select(e => e.LibraryPath));
            descriptor.Properties.Add(new OptionsValue("Extensions", extensionNames, ExtensionSettings.Extensions.Count));
        }

        // Add function info
        if (FunctionRegistry.Functions.Any())
        {
            var functionNames = string.Join(", ", FunctionRegistry.Functions.Select(f => f.Name));
            descriptor.Properties.Add(new OptionsValue("Functions", functionNames, FunctionRegistry.Functions.Count));
        }

        descriptor.Properties.RemoveAll(x => x.Name.ContainsIgnoreCase("password"));

        return descriptor;
    }
}
