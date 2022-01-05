using System;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Baseline;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    [Collection("integration")]
    public abstract class IntegrationContext : IDisposable, IAsyncLifetime
    {
        private readonly string _schemaName;
        protected readonly SqlConnection theConnection = new SqlConnection(ConnectionSource.ConnectionString);

        protected IntegrationContext(string schemaName)
        {
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
            var rules = new SqlServerMigrator();
            var writer = new StringWriter();
            schemaObject.WriteCreateStatement(rules, writer);

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
            var rules = new SqlServerMigrator();
            var writer = new StringWriter();
            schemaObject.WriteDropStatement(rules, writer);

            return theConnection.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }

        public virtual Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public virtual Task DisposeAsync()
        {
            return Task.CompletedTask;
        }
    }
}
