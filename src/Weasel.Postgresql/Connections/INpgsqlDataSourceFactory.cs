using Npgsql;

namespace Weasel.Postgresql.Connections;

public interface INpgsqlDataSourceFactory
{
    NpgsqlDataSource Create(string connectionString);


    NpgsqlDataSource Create(string masterConnectionString, string databaseName);
}
