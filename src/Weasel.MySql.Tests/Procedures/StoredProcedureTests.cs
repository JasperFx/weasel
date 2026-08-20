using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.MySql.Procedures;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Procedures;

/// <summary>
///     MySQL stored procedure support (weasel#451). MySQL stores a routine's body verbatim in
///     <c>information_schema.ROUTINES.ROUTINE_DEFINITION</c> — unlike a view definition, which it
///     rewrites — so comparison is a straight match on the body.
/// </summary>
/// <remarks>
///     A procedure body is the case that made the migrator stop splitting delta SQL on semicolons
///     in weasel#452: every <c>BEGIN … END</c> block it saw got shredded.
/// </remarks>
[Collection("integration")]
public class StoredProcedureTests: IAsyncLifetime
{
    private const string SchemaName = "weasel_testing";

    private MySqlConnection theConnection = default!;

    public async ValueTask InitializeAsync()
    {
        var builder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            UserID = "root", Password = "P@55w0rd", Database = SchemaName
        };

        theConnection = new MySqlConnection(builder.ConnectionString);
        await theConnection.OpenAsync();
        await cleanupAsync();

        var table = new Table($"{SchemaName}.sp_log");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("note");
        await table.ApplyChangesAsync(theConnection);
    }

    public async ValueTask DisposeAsync()
    {
        await cleanupAsync();
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    private async Task cleanupAsync()
    {
        await theConnection.CreateCommand($"DROP PROCEDURE IF EXISTS `{SchemaName}`.`sp_stamp`")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand($"DROP TABLE IF EXISTS `{SchemaName}`.`sp_log`")
            .ExecuteNonQueryAsync();
    }

    private static StoredProcedure NewProcedure(string note = "touched") => new(
        $"{SchemaName}.sp_stamp",
        $@"CREATE PROCEDURE `{SchemaName}`.`sp_stamp`()
BEGIN
    INSERT INTO `{SchemaName}`.`sp_log` (note) VALUES ('{note}');
END");

    [Fact]
    public void the_body_is_taken_from_the_first_begin()
    {
        StoredProcedure.ExtractBody("CREATE PROCEDURE x() BEGIN SELECT 1; END;")
            .ShouldBe("BEGIN SELECT 1; END");
    }

    [Fact]
    public async Task a_procedure_round_trips_and_reports_no_delta()
    {
        var procedure = NewProcedure();
        await procedure.ApplyChangesAsync(theConnection);

        (await procedure.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await procedure.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_procedure_does_not_report_permanent_drift()
    {
        await NewProcedure().ApplyChangesAsync(theConnection);

        (await NewProcedure().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await NewProcedure().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The body contains semicolons, which is the whole point: the migrator used to split delta
    ///     SQL on them and execute the fragments.
    /// </summary>
    [Fact]
    public async Task the_procedure_actually_runs()
    {
        await NewProcedure().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"CALL `{SchemaName}`.`sp_stamp`()").ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand($"SELECT note FROM `{SchemaName}`.`sp_log` LIMIT 1")
            .ExecuteScalarAsync();

        note.ShouldBe("touched");
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await NewProcedure().ApplyChangesAsync(theConnection);

        var changed = NewProcedure("changed");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
