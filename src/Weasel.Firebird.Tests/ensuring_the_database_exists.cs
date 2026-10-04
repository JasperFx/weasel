using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

[Collection("integration")]
public class ensuring_the_database_exists: IAsyncLifetime
{
    private readonly string theConnectionString = ConnectionSource.ForDatabase("ensured");

    public async ValueTask InitializeAsync()
    {
        await dropDatabaseAsync(theConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        await dropDatabaseAsync(theConnectionString);
    }

    /// <summary>
    ///     A8: FirebirdClient 10.3.4's <c>DropDatabase</c> throws <see cref="NullReferenceException" />
    ///     when the file is missing, so "drop it if it is there" has to swallow that too.
    /// </summary>
    private static async Task dropDatabaseAsync(string connectionString)
    {
        FbConnection.ClearAllPools();

        try
        {
            await FbConnection.DropDatabaseAsync(connectionString);
        }
        catch (Exception e) when (e is NullReferenceException or FbException)
        {
        }
    }

    private static async Task<T> scalarAsync<T>(string connectionString, string sql)
    {
        await using var conn = new FbConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand(sql);
        return (T)Convert.ChangeType((await cmd.ExecuteScalarAsync())!, typeof(T));
    }

    [Fact]
    public async Task a_missing_database_is_created_with_16k_pages_and_the_connections_character_set()
    {
        await new FirebirdMigrator().EnsureDatabaseExistsAsync(new FbConnection(theConnectionString));

        (await scalarAsync<int>(theConnectionString, "SELECT MON$PAGE_SIZE FROM MON$DATABASE")).ShouldBe(16384,
            "Quartz's schema does not fit an 8 KB page in a UTF8 database");
        (await scalarAsync<string>(theConnectionString, "SELECT TRIM(RDB$CHARACTER_SET_NAME) FROM RDB$DATABASE"))
            .ShouldBe("UTF8");
    }

    [Fact]
    public async Task the_page_size_is_configurable()
    {
        await new FirebirdMigrator { NewDatabasePageSize = 8192 }
            .EnsureDatabaseExistsAsync(new FbConnection(theConnectionString));

        (await scalarAsync<int>(theConnectionString, "SELECT MON$PAGE_SIZE FROM MON$DATABASE")).ShouldBe(8192);
    }

    /// <summary>
    ///     weasel#647's lesson: the database is opened first, and created only when the file is missing.
    /// </summary>
    [Fact]
    public async Task an_existing_database_is_left_as_it_is()
    {
        await ConnectionSource.CreateDatabaseAsync(theConnectionString);
        await using (var conn = new FbConnection(theConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand("CREATE TABLE kept (id INTEGER)");
            await cmd.ExecuteNonQueryAsync();
        }

        await new FirebirdMigrator().EnsureDatabaseExistsAsync(new FbConnection(theConnectionString));

        (await scalarAsync<int>(theConnectionString,
            "SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'KEPT'")).ShouldBe(1);
    }

    /// <summary>
    ///     A create that loses a race to another process finds the file there after all, which is
    ///     success.
    /// </summary>
    [Fact]
    public async Task racing_ensures_all_succeed()
    {
        var racers = Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() =>
                new FirebirdMigrator().EnsureDatabaseExistsAsync(new FbConnection(theConnectionString))))
            .ToArray();

        await Task.WhenAll(racers);

        (await scalarAsync<int>(theConnectionString, "SELECT 1 FROM RDB$DATABASE")).ShouldBe(1);
    }

    [Fact]
    public async Task a_failure_other_than_a_missing_file_is_not_mistaken_for_one()
    {
        var wrongPassword = new FbConnectionStringBuilder(theConnectionString) { Password = "not-the-password" };

        await Should.ThrowAsync<FbException>(() =>
            new FirebirdMigrator().EnsureDatabaseExistsAsync(new FbConnection(wrongPassword.ConnectionString)));
    }

    [Fact]
    public async Task a_missing_file_and_an_existing_one_are_told_apart()
    {
        var missing = await Should.ThrowAsync<FbException>(async () =>
        {
            await using var conn = new FbConnection(theConnectionString);
            await conn.OpenAsync();
        });
        FirebirdMigrator.IsMissingDatabase(missing).ShouldBeTrue();
        FirebirdMigrator.IsExistingDatabase(missing).ShouldBeFalse();

        await ConnectionSource.CreateDatabaseAsync(theConnectionString);

        var existing = await Should.ThrowAsync<FbException>(() =>
            FbConnection.CreateDatabaseAsync(theConnectionString, 16384, false, false));
        FirebirdMigrator.IsExistingDatabase(existing).ShouldBeTrue();
        FirebirdMigrator.IsMissingDatabase(existing).ShouldBeFalse();
    }
}
