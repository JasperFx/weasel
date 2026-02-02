using System.Data.Common;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Weasel.Core;
using Weasel.Core.Migrations;
using ITable = Weasel.Core.ITable;

namespace Weasel.EntityFrameworkCore;

public record DbContextMigration(DbConnection connection, Migrator Migrator, SchemaMigration Migration);

public static class DbContextExtensions
{
    public static async Task<DbContextMigration> CreateMigrationAsync(
        this IServiceProvider services,
        DbContext context,
        CancellationToken cancellation)
    {
        var migrators = services.GetServices<Migrator>().ToList();
        if (!migrators.Any())
        {
            throw new InvalidOperationException($"No {typeof(Migrator).FullNameInCode()} services are registered!");
        }

        await using var conn = context.Database.GetDbConnection();
        var migrator = migrators.FirstOrDefault(x => x.MatchesConnection(conn));
        if (migrator == null)
        {
            throw new InvalidOperationException(
                $"No matching {typeof(Migrator).FullNameInCode()} instances for DbContext {context}. Registered migrators are {migrators.Select(x => x.ToString()).Join(", ")}");
        }

        var tables = context
            .Model
            .GetEntityTypes()
            .Select(x => migrator.MapToTable(x))
            .OfType<ISchemaObject>()
            .ToArray();

        await conn.OpenAsync(cancellation).ConfigureAwait(false);

        try
        {
            var migration = await SchemaMigration.DetermineAsync(conn, cancellation, tables).ConfigureAwait(false);
            return new DbContextMigration(conn, migrator, migration);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    public static ITable MapToTable(this Migrator migrator, IEntityType entityType)
    {
        throw new NotImplementedException();
    }
}
