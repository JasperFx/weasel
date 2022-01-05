using Baseline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Oakton;
using Weasel.CommandLine.Tests;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;

namespace CommandLineTarget
{
    public class DatabaseCollection
    {
        internal readonly LightweightCache<string, DatabaseWithTables> Databases
            = new LightweightCache<string, DatabaseWithTables>(name =>
                new DatabaseWithTables(AutoCreate.CreateOrUpdate, name));

        public void AddTable(string databaseName, string featureName, string tableName)
        {
            var name = DbObjectName.Parse(PostgresqlProvider.Instance, tableName);
            Databases[databaseName].Features[featureName].AddTable(name.Schema, name.Name);
        }

        public Task<int> Execute(string[] args)
        {
            return Host.CreateDefaultBuilder().ConfigureServices(services =>
            {
                foreach (var database in Databases)
                {
                    services.AddSingleton<IDatabase>(database);
                }
            }).RunOaktonCommands(args);
        }
    }
}
