using JasperFx.Core;

namespace Weasel.Oracle.Tests;

public class ConnectionSource
{
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_oracle_testing_database")
        ?? "Data Source=localhost:1521/FREEPDB1;User Id=weasel;Password=P@55w0rd;";

    static ConnectionSource()
    {
        if (ConnectionString.IsEmpty())
            throw new Exception(
                "You need to set the connection string for your local Oracle database in the environment variable 'weasel_oracle_testing_database'");
    }
}
