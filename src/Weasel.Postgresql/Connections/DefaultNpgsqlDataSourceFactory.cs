using JasperFx.Core;
using Npgsql;

namespace Weasel.Postgresql.Connections;

public class DefaultNpgsqlDataSourceFactory: INpgsqlDataSourceFactory
{
    private readonly Cache<string, NpgsqlDataSourceBuilder> builderCache = new();

    public DefaultNpgsqlDataSourceFactory(Func<string, NpgsqlDataSourceBuilder> dataSourceBuilderFactory)
    {
        builderCache.OnMissing = dataSourceBuilderFactory;
    }

    public DefaultNpgsqlDataSourceFactory(): this(connectionString => new NpgsqlDataSourceBuilder(connectionString))
    {
    }

    public NpgsqlDataSource Create(string connectionString)
    {
        var builder = builderCache[connectionString];

        return builder.Build();
    }

    public NpgsqlDataSource Create(string masterConnectionString, string databaseName)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(masterConnectionString) { Database = databaseName }
            .ConnectionString;

        var builder = builderCache[connectionString];

        return builder.Build();
    }
}
