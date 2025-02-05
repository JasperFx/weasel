using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using JasperFx;
using JasperFx.CommandLine;
using Weasel.Core;
using Weasel.Core.CommandLine;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Weasel.Postgresql.Tests;
using Xunit;

namespace Weasel.CommandLine.Tests;

[Collection("integration")]
public abstract class IntegrationContext
{
    internal readonly LightweightCache<string, DatabaseWithTables> Databases
        = new LightweightCache<string, DatabaseWithTables>(name =>
            new DatabaseWithTables(JasperFx.AutoCreate.CreateOrUpdate, name));

    internal async Task DropSchema(string schemaName)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.DropSchemaAsync(schemaName);
    }

    internal Task<bool> ExecuteCommand<TCommand>() where TCommand : JasperFxAsyncCommand<WeaselInput>, new()
    {
        var command = new TCommand();
        var builder = Host.CreateDefaultBuilder().ConfigureServices(services =>
        {
            foreach (var database in Databases)
            {
                services.AddSingleton<IDatabase>(database);
            }
        });

        var input = new WeaselInput() { HostBuilder = builder };


        return command.Execute(input);
    }

    internal async Task AssertAllDatabasesMatchConfiguration()
    {
        foreach (var database in Databases)
        {
            await database.AssertDatabaseMatchesConfigurationAsync();
        }
    }
}
