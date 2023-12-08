using Npgsql;

namespace Weasel.Postgresql.Connections;

/// <summary>
/// A factory that, by default, just caches the data source it got from outside (e.g. from DI).
/// That means that it's NOT managing its lifetime.
/// It still allows the creation and maintenance of a lifetime of child data sources.
/// </summary>
public class SingleNpgsqlDataSourceFactory(
    Func<string, NpgsqlDataSourceBuilder> dataSourceBuilderFactory,
    NpgsqlDataSource dataSource
): DefaultNpgsqlDataSourceFactory(dataSourceBuilderFactory)
{
    public override NpgsqlDataSource Create(string connectionString) =>
        dataSource.ConnectionString.Equals(connectionString)
            ? dataSource
            : base.Create(connectionString);
}
