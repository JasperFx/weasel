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

    /// <summary>
    /// The wrapped data source was supplied from the outside (e.g. from DI), so this factory does NOT own it.
    /// Any child data sources it builds itself from other connection strings are owned as usual.
    /// </summary>
    public override bool OwnsDataSource(NpgsqlDataSource candidate) =>
        !ReferenceEquals(candidate, dataSource) && base.OwnsDataSource(candidate);
}
