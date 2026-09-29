using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Procedures;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Procedures;

/// <summary>
///     Firebird keeps a procedure's body as source and its parameters in
///     <c>RDB$PROCEDURE_PARAMETERS</c>, so a changed parameter is as much a change as a changed body.
///     The bodies are full of semicolons, which is what the <c>SET TERM</c> wrapping is for.
/// </summary>
public class procedures_in_the_database: IntegrationContext
{
    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        var log = new Table("sp_log");
        log.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        log.AddColumn("note", "VARCHAR(40)");
        await ApplyAsync(log);
    }

    private static StoredProcedure touch(string note = "touched") => new("sp_touch", $"""
        CREATE PROCEDURE sp_touch
        AS
        BEGIN
            INSERT INTO sp_log (note) VALUES ('{note}');
        END
        """);

    private static StoredProcedure stamp(string outputs = "id INTEGER, stamped VARCHAR(40) NOT NULL") => new("sp_stamp", $"""
        CREATE PROCEDURE sp_stamp (note VARCHAR(40) = 'stamped; twice', times INTEGER NOT NULL DEFAULT 1)
        RETURNS ({outputs})
        AS
            DECLARE VARIABLE i INTEGER = 0;
        BEGIN
            WHILE (i < times) DO
            BEGIN
                INSERT INTO sp_log (note) VALUES (:note) RETURNING id INTO :id;
                stamped = note;
                i = i + 1;
                SUSPEND;
            END
        END
        """);

    [Fact]
    public async Task a_missing_procedure_reports_create_and_applying_it_converges()
    {
        var procedure = touch();

        (await procedure.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await procedure.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);

        await ApplyAsync(procedure);

        (await procedure.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_executable_procedure_actually_runs()
    {
        await ApplyAsync(touch());

        await ExecuteAsync("EXECUTE PROCEDURE sp_touch");

        (await ListAsync("SELECT note FROM sp_log")).ShouldBe(["touched"]);
    }

    [Fact]
    public async Task a_selectable_procedure_actually_returns_rows()
    {
        await ApplyAsync(stamp());

        (await ListAsync("SELECT stamped FROM sp_stamp('hello', 2)")).ShouldBe(["hello", "hello"]);
        (await ListAsync("SELECT stamped FROM sp_stamp")).ShouldBe(["stamped; twice"]);
    }

    [Fact]
    public async Task an_unchanged_procedure_does_not_report_permanent_drift()
    {
        await ApplyAsync(stamp(), touch());

        (await DetermineAsync(stamp(), touch())).Difference.ShouldBe(SchemaPatchDifference.None);

        var delta = await stamp().FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None, delta.ToString());
        (await touch().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ApplyAsync(touch());

        var changed = touch("changed");
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await ExecuteAsync("EXECUTE PROCEDURE sp_touch");
        (await ListAsync("SELECT note FROM sp_log")).ShouldBe(["changed"]);
    }

    [Theory]
    [InlineData("id INTEGER, stamped VARCHAR(40)")]
    [InlineData("id BIGINT, stamped VARCHAR(40) NOT NULL")]
    [InlineData("id INTEGER, stamped VARCHAR(40) NOT NULL, extra INTEGER")]
    [InlineData("stamped VARCHAR(40) NOT NULL, id INTEGER")]
    public async Task a_changed_output_parameter_reports_update_and_applying_it_converges(string outputs)
    {
        await ApplyAsync(stamp());

        var changed = stamp(outputs);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_input_default_is_a_change()
    {
        await ApplyAsync(stamp());

        var changed = new StoredProcedure("sp_stamp",
            stamp().BodyText().Replace("'stamped; twice'", "'stamped once'"));

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task an_update_keeps_a_procedure_that_calls_it()
    {
        await ApplyAsync(stamp());
        await ExecuteAsync("""
            SET TERM ^ ;
            CREATE PROCEDURE sp_caller RETURNS (id INTEGER) AS
            BEGIN
              FOR SELECT id FROM sp_stamp('from the caller', 1) INTO :id DO SUSPEND;
            END
            ^
            SET TERM ; ^
            """);

        await ApplyAsync(stamp("id INTEGER, stamped VARCHAR(40) NOT NULL, extra INTEGER"));

        (await ListAsync("SELECT id FROM sp_caller")).Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_removed_procedure_is_dropped()
    {
        await ApplyAsync(touch());

        var removed = touch();
        removed.IsRemoved = true;
        (await removed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(removed);

        (await touch().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task fetch_existing_rebuilds_a_statement_that_recreates_the_procedure()
    {
        await ApplyAsync(stamp());

        var existing = await stamp().FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();

        await stamp().DropAsync(theConnection);
        await existing!.CreateAsync(theConnection);

        var delta = await stamp().FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None, delta.ToString());
    }

    [Fact]
    public async Task rolling_back_an_update_restores_the_previous_procedure()
    {
        await ApplyAsync(stamp());

        var migration = await ApplyAsync(stamp("id BIGINT, stamped VARCHAR(40) NOT NULL"));
        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await stamp().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_procedures_with_it()
    {
        await ApplyAsync(stamp(), touch());
        await ApplyAsync(new StoredProcedure("sp_both", """
            CREATE PROCEDURE sp_both AS
                DECLARE VARIABLE id INTEGER;
            BEGIN
                EXECUTE PROCEDURE sp_touch;
                SELECT FIRST 1 id FROM sp_stamp INTO :id;
            END
            """));

        await theConnection.DropSchemaAsync();

        (await stamp().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await touch().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
