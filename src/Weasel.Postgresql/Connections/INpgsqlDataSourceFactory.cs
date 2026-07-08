using Npgsql;

namespace Weasel.Postgresql.Connections;

public interface INpgsqlDataSourceFactory
{
    NpgsqlDataSource Create(string connectionString);

    /// <summary>
    /// Does this factory own the lifetime of the supplied <see cref="NpgsqlDataSource"/>? A factory that
    /// builds a data source from a connection string owns it; one that merely wraps a caller-supplied data
    /// source does not. When this returns <c>false</c>, consumers (e.g. <see cref="PostgresqlDatabase"/>) must
    /// NOT dispose the data source because its lifetime is owned externally — disposing it would abort every
    /// physical connection rented from it, including those still in use elsewhere.
    /// </summary>
    bool OwnsDataSource(NpgsqlDataSource dataSource) => true;
}

public static class NpgsqlDataSourceFactoryExtensions
{
    public static NpgsqlDataSource Create(
        this INpgsqlDataSourceFactory dataSourceFactory,
        string masterConnectionString,
        string databaseName
    )
    {
        var builder = new NpgsqlConnectionStringBuilder(masterConnectionString) { Database = databaseName };

        return dataSourceFactory.Create(builder.ConnectionString);
    }
}
