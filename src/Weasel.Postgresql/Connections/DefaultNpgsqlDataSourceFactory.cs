using JasperFx.Core;
using Npgsql;

namespace Weasel.Postgresql.Connections;

public class DefaultNpgsqlDataSourceFactory: INpgsqlDataSourceFactory, IAsyncDisposable, IDisposable
{
    protected readonly Cache<string, NpgsqlDataSourceBuilder> Builders = new();
    protected readonly Cache<string, NpgsqlDataSource> DataSources = new();

    public DefaultNpgsqlDataSourceFactory(Func<string, NpgsqlDataSourceBuilder> dataSourceBuilderFactory)
    {
        Builders.OnMissing = dataSourceBuilderFactory;
        DataSources.OnMissing = connectionString =>
        {
            var builder = Builders[connectionString];
            return builder.Build();
        };
    }

    public DefaultNpgsqlDataSourceFactory(): this(connectionString => new NpgsqlDataSourceBuilder(connectionString))
    {
    }

    public virtual NpgsqlDataSource Create(string connectionString) =>
        DataSources[connectionString];

    public virtual void Dispose()
    {
        var dataSources = DataSources.ToList();
        DataSources.ClearAll();

        foreach (var dataSource in dataSources)
        {
            dataSource.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var dataSources = DataSources.ToList();
        DataSources.ClearAll();

        foreach (var dataSource in dataSources)
        {
            await dataSource.DisposeAsync().ConfigureAwait(false);
        }
    }
}
