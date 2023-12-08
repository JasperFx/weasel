using Npgsql;

namespace Weasel.Postgresql.Connections;

public interface INpgsqlDataSourceFactory
{
    NpgsqlDataSource Create(string connectionString);
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
