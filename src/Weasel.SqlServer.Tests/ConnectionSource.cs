using Baseline;

namespace Weasel.SqlServer.Tests;

public class ConnectionSource
{
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_sqlserver_testing_database")
        ?? "Server=localhost;User Id=sa;Password=P@55w0rd;Timeout=5;TrustServerCertificate=true";

    static ConnectionSource()
    {
        if (ConnectionString.IsEmpty())
            throw new Exception(
                "You need to set the connection string for your local Postgresql database in the environment variable 'weasel_postgresql_testing_database'");
    }
}
