using JasperFx.Core;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     The named constants and the fluent methods over Table.StorageParameters, and the duplicate a
///     case-sensitive key makes reachable without them.
/// </summary>
public class storage_parameter_names_and_fluent_api
{
    private static Table newTable()
    {
        var table = new Table("storage_parameter_api.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    private static string ddlFor(Table table)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(new PostgresqlMigrator(), writer);
        return writer.ToString();
    }

    [Fact]
    public void every_constant_is_the_lower_case_reloption_name()
    {
        // The DDL writer and the catalog reader both normalize to lower case, so a constant that is
        // not already lower case would be a key that never matches what comes back.
        var constants = typeof(StorageParameterNames)
            .GetFields()
            .Where(x => x is { IsLiteral: true, IsStatic: true } && x.FieldType == typeof(string))
            .Select(x => (x.Name, Value: (string)x.GetRawConstantValue()!))
            .ToArray();

        constants.ShouldNotBeEmpty();
        constants.ShouldAllBe(x => x.Value == x.Value.ToLowerInvariant());

        // toast.* parameters live on the TOAST relation, and DeclaredStorageParameters refuses them,
        // so offering one as a constant would be offering a name that cannot work.
        constants.ShouldNotContain(x => x.Value.StartsWith("toast."));
    }

    [Fact]
    public void fill_factor_round_trips_through_the_constant()
    {
        var table = newTable();
        table.FillFactor = 80;

        table.StorageParameters[StorageParameterNames.FillFactor].ShouldBe(80);
        table.FillFactor.ShouldBe(80);
    }

    [Fact]
    public void with_storage_parameter_normalizes_the_name()
    {
        var table = newTable().WithStorageParameter("FillFactor", 70);

        table.StorageParameters[StorageParameterNames.FillFactor].ShouldBe(70);
        ddlFor(table).ShouldContain(") WITH (fillfactor = 70);");
    }

    [Fact]
    public void with_storage_parameter_removes_on_null()
    {
        var table = newTable().WithFillFactor(70).WithFillFactor(null);

        table.StorageParameters.Count.ShouldBe(0);
        ddlFor(table).ShouldNotContain("WITH (");
    }

    [Fact]
    public void the_fluent_methods_declare_only_what_was_supplied()
    {
        // A null argument means "say nothing about this one". It cannot mean "reset it", because the
        // delta never resets a parameter the table does not declare.
        var table = newTable().WithAutovacuum(vacuumScaleFactor: 0.01, insertScaleFactor: 0.02);

        table.StorageParameters.Count.ShouldBe(2);
        ddlFor(table).ShouldContain(
            ") WITH (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.02);");
    }

    [Fact]
    public void the_fluent_methods_compose_and_return_the_table()
    {
        var table = newTable()
            .WithFillFactor(70)
            .WithAutovacuum(enabled: true, vacuumThreshold: 1000)
            .WithParallelWorkers(4)
            .WithStorageParameter(StorageParameterNames.VacuumTruncate, false);

        var ddl = ddlFor(table);
        ddl.ShouldContain("fillfactor = 70");
        ddl.ShouldContain("autovacuum_enabled = True");
        ddl.ShouldContain("autovacuum_vacuum_threshold = 1000");
        ddl.ShouldContain("parallel_workers = 4");
        ddl.ShouldContain("vacuum_truncate = False");
    }

    [Fact]
    public void autovacuum_logging_takes_a_timespan()
    {
        newTable().WithAutovacuumLogging(250.Milliseconds())
            .StorageParameters[StorageParameterNames.LogAutovacuumMinDuration].ShouldBe(250);

        // PostgreSQL spells "never log one" as -1, not as a duration
        newTable().WithAutovacuumLogging(Timeout.InfiniteTimeSpan)
            .StorageParameters[StorageParameterNames.LogAutovacuumMinDuration].ShouldBe(-1);

        newTable().WithAutovacuumLogging(TimeSpan.Zero)
            .StorageParameters[StorageParameterNames.LogAutovacuumMinDuration].ShouldBe(0);
    }

    [Fact]
    public void two_spellings_of_one_parameter_are_refused_rather_than_duplicated()
    {
        // StorageParameters keys are case sensitive but are lowered when written, so these are two
        // entries that would render as "fillfactor = 90, fillfactor = 70" and PostgreSQL would
        // reject the whole statement with 22023. Refuse it here, where the message can say why.
        var table = newTable();
        table.StorageParameters["FILLFACTOR"] = 90;
        table.StorageParameters["fillfactor"] = 70;

        var exception = Should.Throw<InvalidOperationException>(() => ddlFor(table));
        exception.Message.ShouldContain("more than once");
        exception.Message.ShouldContain(nameof(StorageParameterNames));
    }
}
