using Microsoft.EntityFrameworkCore;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.EntityFrameworkCore;

public static class DbContextExtensions
{
    public static async Task<SchemaMigration> CreateMigrationAsync(this DbContext context, IDatabase database,
        CancellationToken cancellation)
    {
        foreach (var entityType in context.Model.GetEntityTypes())
        {

        }
    }
}
