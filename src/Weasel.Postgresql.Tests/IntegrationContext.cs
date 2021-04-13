using System;
using System.IO;
using System.Threading.Tasks;
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

        protected Task CreateSchemaObjectInDatabase(ISchemaObject schemaObject)
        {
            var rules = new DdlRules();
            var writer = new StringWriter();
            schemaObject.Write(rules, writer);

            return theConnection.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }

        protected Task DropSchemaObjectInDatabase(ISchemaObject schemaObject)
        {
            var rules = new DdlRules();
            var writer = new StringWriter();
            schemaObject.WriteDropStatement(rules, writer);

            return theConnection.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }
    }
}