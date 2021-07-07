using System;
using Baseline;

namespace Weasel.Postgresql.Tests
{
    public class ConnectionSource 
    {
        // TODO -- use a separate database
        public static readonly string ConnectionString = Environment.GetEnvironmentVariable("weasel_postgresql_testing_database")
            ?? "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;password=postgres";

        static ConnectionSource()
        {
            if (ConnectionString.IsEmpty())
                throw new Exception(
                    "You need to set the connection string for your local Postgresql database in the environment variable 'weasel_postgresql_testing_database'");
        }
        
    }
}
