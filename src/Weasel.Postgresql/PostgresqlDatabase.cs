using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Functions;
using Table = Weasel.Postgresql.Tables.Table;

namespace Weasel.Postgresql;

public abstract class PostgresqlDatabase: DatabaseBase<NpgsqlConnection>, IAsyncDisposable
{
    protected PostgresqlDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        NpgsqlDataSource dataSource
    ): base(logger, autoCreate, migrator, identifier, () => dataSource.CreateConnection())
    {
        DataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));

        // ReSharper disable once VirtualMemberCallInConstructor
        var descriptor = Describe();
        Id = new DatabaseId(descriptor.ServerName, descriptor.DatabaseName);
    }

    public override DatabaseDescriptor Describe()
    {
        var builder = new NpgsqlConnectionStringBuilder(DataSource.ConnectionString);
        var descriptor = new DatabaseDescriptor()
        {
            Engine = PostgresqlProvider.EngineName,
            ServerName = ParsePostgresHost(builder.Host),
            DatabaseName = builder.Database ?? string.Empty,
            Subject = GetType().FullNameInCode(),
            Identifier = Identifier
        };

        descriptor.TenantIds.AddRange(TenantIds);

        return descriptor;
    }

    private static string ParsePostgresHost(string? host)
    {
        if (string.IsNullOrEmpty(host))
            return string.Empty;

        // if the host isn't a multihost string we don't have to do anything
        if (!host.Contains(','))
            return host;

        var hostSpan = host.AsSpan();

        var commaIndex = hostSpan.IndexOf(',');
        var firstHost = hostSpan[..commaIndex];

        var colonIndex = hostSpan.IndexOf(':');
        // check if there's any inline port definitions
        if (colonIndex != -1)
        {
            return new string(firstHost[..colonIndex]);
        }

        return new string(firstHost);

    }

    public NpgsqlDataSource DataSource { get; }

    protected override ISchemaObject[] possiblyCheckForSchemas(ISchemaObject[] objects)
    {
        return SchemaExistenceCheck.WithSchemaCheck(objects);
    }

    public async Task<Function?> DefinitionForFunction(DbObjectName function, CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await conn.FindExistingFunction(function, ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Fetch a list of the existing tables in the database
    /// </summary>
    /// <param name="database"></param>
    /// <returns></returns>
    public async Task<IReadOnlyList<DbObjectName>> SchemaTables(CancellationToken ct = default)
    {
        var schemaNames = AllSchemaNames();

        await using var conn = CreateConnection();

        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await conn.ExistingTablesAsync(schemas: schemaNames, ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a connection from the underlying <see cref="NpgsqlDataSource"/>
    /// </summary>
    /// <param name="targetSessionAttributes">Sets the target session attributes. Only relevant when <see cref="NpgsqlMultiHostDataSource"/> is being used.</param>
    /// <returns></returns>
    public NpgsqlConnection CreateConnection(TargetSessionAttributes targetSessionAttributes = TargetSessionAttributes.Primary)
    {
        if (DataSource is NpgsqlMultiHostDataSource multiHostDataSource)
        {
            return multiHostDataSource.CreateConnection(targetSessionAttributes);
        }

        return base.CreateConnection();
    }

    public override NpgsqlConnection CreateConnection()
    {
        return CreateConnection();
    }

    public ValueTask DisposeAsync()
    {
        return DataSource.DisposeAsync();
    }

    public async Task<Table[]> FetchExistingTablesAsync()
    {
        var tableNames = AllObjects().OfType<Table>().Select(x => x.Identifier).ToArray();

        var list = new List<Table>();
        await using var conn = CreateConnection();
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        foreach (var tableName in tableNames)
        {
            var table = await new Table(tableName).FetchExistingAsync(conn, CancellationToken.None).ConfigureAwait(false);
            if (table != null)
            {
                list.Add(table);
            }
        }

        return list.ToArray();
    }
}
