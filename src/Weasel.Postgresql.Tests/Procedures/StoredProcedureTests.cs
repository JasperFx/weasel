using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Procedures;
using Xunit;

namespace Weasel.Postgresql.Tests.Procedures;

/// <summary>
///     PostgreSQL stored procedure support (weasel#451). Real <c>PROCEDURE</c> objects, distinct
///     from functions: no return value, and they can manage transactions.
/// </summary>
[Collection("procedures")]
public class StoredProcedureTests: IntegrationContext
{
    public StoredProcedureTests(): base("procedures")
    {
    }

    private static StoredProcedure NewProcedure(string note = "touched") => new(
        "procedures.stamp",
        $@"CREATE OR REPLACE PROCEDURE procedures.stamp(n int) LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO procedures.log (note) VALUES ('{note}');
END;
$$;");

    private async Task createLogTableAsync()
    {
        await theConnection.CreateCommand(
                "create table procedures.log (id serial primary key, note varchar(100))")
            .ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     The reason comparison uses <c>prosrc</c> rather than <c>pg_get_functiondef</c>: the body
    ///     comes back verbatim, and dollar quoting makes extracting it from the caller's statement
    ///     unambiguous.
    /// </summary>
    [Fact]
    public void the_body_is_taken_from_between_the_dollar_quotes()
    {
        StoredProcedure.ExtractBody("CREATE PROCEDURE x() AS $$ BEGIN NULL; END; $$;")
            .Trim().ShouldBe("BEGIN NULL; END;");
    }

    [Fact]
    public void a_tagged_dollar_quote_is_handled_too()
    {
        StoredProcedure.ExtractBody("CREATE PROCEDURE x() AS $body$ BEGIN NULL; END; $body$;")
            .Trim().ShouldBe("BEGIN NULL; END;");
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

    /// <summary>
    ///     Checked twice, because comparing against PostgreSQL's rendered definition rather than the
    ///     stored body would pass once and then report drift forever.
    /// </summary>
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

        await theConnection.CreateCommand("call procedures.stamp(1)").ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand("select note from procedures.log limit 1")
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
