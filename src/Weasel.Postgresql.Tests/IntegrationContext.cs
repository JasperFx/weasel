using System;
using Baseline;
using Marten.Testing.Harness;
using Npgsql;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public abstract class IntegrationContext : IDisposable
    {
        protected readonly NpgsqlConnection theConnection = new NpgsqlConnection(ConnectionSource.ConnectionString);
        
        protected IntegrationContext(string schemaName)
        {
            if (!GetType().HasAttribute<CollectionAttribute>())
            {
                throw new InvalidOperationException("You must decorate this class with a [Collection(\"schemaname\"] attribute. Preferably w/ the schema name");
            }
        }

        public void Dispose()
        {
            theConnection?.Dispose();
        }
    }
}