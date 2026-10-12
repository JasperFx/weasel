using JasperFx;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
///     weasel#706. <c>WriteMigrationFileAsync</c>, which <c>db-patch</c> calls, prefixed the patch with a
///     <c>CREATE SCHEMA</c> and the drop file with <c>drop schema if exists ... CASCADE</c> for every schema
///     the application knows about, not just the ones the patch changes. Running the drop file as a
///     rollback cascaded through schemas the patch never touched and destroyed their tables and data.
/// </summary>
[Collection("migrations")]
public class migration_files_leave_untouched_schemas_alone : IAsyncLifetime
{
    private const string UntouchedSchema = "gh706_untouched";
    private const string FeatureSchema = "gh706_feature";

    private readonly NpgsqlDataSource theDataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
    private readonly string theFile = Path.Combine(Path.GetTempPath(), $"weasel-gh706-{Guid.NewGuid():n}.sql");

    public async ValueTask InitializeAsync()
    {
        await executeAsync($"drop schema if exists {UntouchedSchema} cascade; drop schema if exists {FeatureSchema} cascade;");
    }

    public async ValueTask DisposeAsync()
    {
        await executeAsync($"drop schema if exists {UntouchedSchema} cascade; drop schema if exists {FeatureSchema} cascade;");
        File.Delete(theFile);
        File.Delete(Weasel.Core.SchemaMigration.ToDropFileName(theFile));
        await theDataSource.DisposeAsync();
    }

    [Fact]
    public async Task the_drop_file_only_rolls_back_what_the_patch_changed()
    {
        var database = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "gh706", theDataSource);

        // An already migrated table in a schema the patch will not touch, holding data
        database.Features["Existing"].AddTable(UntouchedSchema, "kept");
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
        await executeAsync($"insert into {UntouchedSchema}.kept (id, name) values ('{Guid.NewGuid()}', 'still here')");

        // The patch adds one table in a new schema
        database.Features["New"].AddTable(FeatureSchema, "new_table");
        await database.WriteMigrationFileAsync(theFile);

        var patch = await File.ReadAllTextAsync(theFile);
        var drop = await File.ReadAllTextAsync(Weasel.Core.SchemaMigration.ToDropFileName(theFile));

        patch.ShouldNotContain(UntouchedSchema);
        drop.ShouldNotContain("drop schema", Case.Insensitive);
        drop.ShouldNotContain(UntouchedSchema);

        await executeAsync(patch);
        (await scalarAsync($"select count(*) from information_schema.tables where table_schema = '{FeatureSchema}' and table_name = 'new_table'")).ShouldBe(1);

        await executeAsync(drop);

        (await scalarAsync($"select count(*) from information_schema.tables where table_schema = '{FeatureSchema}' and table_name = 'new_table'")).ShouldBe(0);
        (await scalarAsync($"select count(*) from {UntouchedSchema}.kept")).ShouldBe(1);
    }

    private async Task executeAsync(string sql)
    {
        await using var command = theDataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> scalarAsync(string sql)
    {
        await using var command = theDataSource.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
}
