using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Procedures;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Procedures;

/// <summary>
///     Oracle stored procedure support (weasel#451). Oracle keeps the source verbatim in
///     <c>all_source</c>, one row per line — and without the <c>CREATE OR REPLACE</c> prefix, which
///     is a wrapper on the statement rather than part of the object.
/// </summary>
[Collection("integration")]
public class StoredProcedureTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public StoredProcedureTests(): base(SchemaName)
    {
    }

    private static StoredProcedure NewProcedure(string note = "touched") => new(
        $"{SchemaName}.sp_stamp",
        $@"CREATE OR REPLACE PROCEDURE {SchemaName}.sp_stamp IS
BEGIN
    INSERT INTO {SchemaName}.sp_log (id, note) VALUES (1, '{note}');
END;");

    private async Task createLogTableAsync()
    {
        var table = new Table($"{SchemaName}.sp_log");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("note");
        await table.ApplyChangesAsync(theConnection);
    }

    /// <summary>
    ///     Oracle stores neither the <c>CREATE OR REPLACE</c> wrapper nor the schema qualifier: the
    ///     owner is already the <c>all_source</c> row's own column.
    /// </summary>
    [Fact]
    public void the_create_or_replace_prefix_and_the_schema_qualifier_are_stripped()
    {
        StoredProcedure.StripCreateOrReplace("CREATE OR REPLACE PROCEDURE WEASEL.x IS BEGIN NULL; END;", "WEASEL")
            .ShouldStartWith("PROCEDURE x");
    }

    [Fact]
    public async Task a_procedure_round_trips_and_reports_no_delta()
    {
        await ResetSchema();
        await createLogTableAsync();

        var procedure = NewProcedure();
        await procedure.ApplyChangesAsync(theConnection);

        (await procedure.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await procedure.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_procedure_does_not_report_permanent_drift()
    {
        await ResetSchema();
        await createLogTableAsync();
        await NewProcedure().ApplyChangesAsync(theConnection);

        (await NewProcedure().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await NewProcedure().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_procedure_actually_runs()
    {
        await ResetSchema();
        await createLogTableAsync();
        await NewProcedure().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"BEGIN {SchemaName}.sp_stamp; END;").ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand($"SELECT note FROM {SchemaName}.sp_log WHERE ROWNUM = 1")
            .ExecuteScalarAsync();

        note.ShouldBe("touched");
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await createLogTableAsync();
        await NewProcedure().ApplyChangesAsync(theConnection);

        var changed = NewProcedure("changed");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_procedures_with_it()
    {
        await ResetSchema();
        await createLogTableAsync();
        await NewProcedure().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewProcedure().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
