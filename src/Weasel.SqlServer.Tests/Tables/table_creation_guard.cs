using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     The <c>IF OBJECT_ID(...) IS NULL</c> that guards <c>CREATE TABLE</c>. It was written without
///     <c>N</c> or an object type: a non-Latin name came out as <c>??</c> in the literal so the guard
///     never found the table, and any object of the same name, a view say, skipped the create.
/// </summary>
public class table_creation_guard: IntegrationContext
{
    private const string Schema = "create_guard";

    public table_creation_guard(): base(Schema)
    {
    }

    private static string createSql(Table table)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    [Fact]
    public async Task the_create_for_a_non_latin_name_runs_twice()
    {
        await ResetSchema();

        var table = new Table(new SqlServerObjectName(Schema, "注文"));
        table.AddColumn<int>("id").AsPrimaryKey();
        var sql = createSql(table);

        await theConnection.CreateCommand(sql).ExecuteNonQueryAsync();

        // Pre-fix: "There is already an object named '注文' in the database."
        await theConnection.CreateCommand(sql).ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task a_view_of_the_same_name_fails_the_create_rather_than_skipping_it()
    {
        await ResetSchema();

        await theConnection.CreateCommand($"create view {Schema}.orders as select 1 as id").ExecuteNonQueryAsync();

        var table = new Table(new SqlServerObjectName(Schema, "orders"));
        table.AddColumn<int>("id").AsPrimaryKey();

        // Pre-fix this ran without error and left no table behind
        var exception = await Should.ThrowAsync<SqlException>(
            async () => await theConnection.CreateCommand(createSql(table)).ExecuteNonQueryAsync());

        exception.Message.ShouldContain("There is already an object named 'orders'");
    }
}
