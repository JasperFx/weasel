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
    }

    public override DatabaseDescriptor Describe()
    {
        var builder = new NpgsqlConnectionStringBuilder(DataSource.ConnectionString);
        var descriptor = new DatabaseDescriptor()
        {
            Engine = PostgresqlProvider.EngineName,
            ServerName = builder.Host ?? string.Empty,
            DatabaseName = builder.Database ?? string.Empty,
            Subject = GetType().FullNameInCode(),
            Identifier = Identifier
        };

        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Host));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Port));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Database));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Username));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ApplicationName));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Enlist));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.SearchPath));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ClientEncoding));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Encoding));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Timezone));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.SslMode));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.SslNegotiation));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.CheckCertificateRevocation));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.KerberosServiceName));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.IncludeRealm));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.PersistSecurityInfo));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.LogParameters));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.IncludeErrorDetail));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ChannelBinding));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Pooling));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.MinPoolSize));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.MaxPoolSize));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ConnectionIdleLifetime));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ConnectionPruningInterval));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ConnectionLifetime));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Timeout));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.CommandTimeout));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.CancellationTimeout));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.TargetSessionAttributes));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.LoadBalanceHosts));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.HostRecheckSeconds));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.KeepAlive));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.TcpKeepAlive));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.TcpKeepAliveTime));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.TcpKeepAliveInterval));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ReadBufferSize));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.WriteBufferSize));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.SocketReceiveBufferSize));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.SocketSendBufferSize));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.MaxAutoPrepare));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.AutoPrepareMinUsages));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.NoResetOnClose));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Options));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ArrayNullabilityMode));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.Multiplexing));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.WriteCoalescingBufferThresholdBytes));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.LoadTableComposites));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.ServerCompatibilityMode));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.TrustServerCertificate));
        descriptor.Properties.Add(OptionsValue.Read(builder, x => x.InternalCommandTimeout));

        descriptor.Properties.RemoveAll(x => x.Name.ContainsIgnoreCase("password"));
        descriptor.Properties.RemoveAll(x => x.Name.ContainsIgnoreCase("certificate"));

        return descriptor;
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
