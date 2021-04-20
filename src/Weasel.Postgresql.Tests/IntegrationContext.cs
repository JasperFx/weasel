using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Baseline;
using Marten.Testing.Harness;
using Npgsql;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public abstract class IntegrationContext : IDisposable
    {
        private readonly string _schemaName;
        protected readonly NpgsqlConnection theConnection = new NpgsqlConnection(ConnectionSource.ConnectionString);
        
        protected IntegrationContext(string schemaName)
        {
            if (!GetType().HasAttribute<CollectionAttribute>())
            {
                throw new InvalidOperationException("You must decorate this class with a [Collection(\"schemaname\"] attribute. Preferably w/ the schema name");
            }

            _schemaName = schemaName;
        }

        public void Dispose()
        {
            theConnection?.Dispose();
        }
        
        protected async Task ResetSchema()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema(_schemaName);
        }

        protected async Task CreateSchemaObjectInDatabase(ISchemaObject schemaObject)
        {
            var rules = new DdlRules();
            var writer = new StringWriter();
            schemaObject.Write(rules, writer);

            try
            {
                await theConnection.CreateCommand(writer.ToString())
                    .ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                throw new Exception("DDL Execution Failure.\n" + writer.ToString(), e);
            }
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