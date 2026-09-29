using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     Firebird's computed columns -- <c>COMPUTED BY (expression)</c>, evaluated when the row is read and
///     never stored -- written, read back from <c>RDB$FIELDS.RDB$COMPUTED_SOURCE</c> and compared, as
///     MySQL's generated columns are.
/// </summary>
public class computed_columns: IntegrationContext
{
    private static Table lines(string totalType = "BIGINT", string expression = "quantity * price")
    {
        var table = new Table("order_lines");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("quantity");
        table.AddColumn<int>("price");
        table.AddColumn("total", totalType).ComputedBy(expression);
        return table;
    }

    [Fact]
    public async Task a_computed_column_round_trips_and_computes()
    {
        var table = lines();
        await ApplyAsync(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.ColumnFor("total")!.ComputedExpression.ShouldBe("quantity * price");

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        await ExecuteAsync("INSERT INTO order_lines (id, quantity, price) VALUES (1, 3, 5)");
        (await ScalarAsync<long>("SELECT total FROM order_lines")).ShouldBe(15L);
    }

    [Fact]
    public async Task a_computed_column_does_not_drift_under_drift_detection()
    {
        var table = lines();
        table.DetectColumnDrift = true;
        table.ModifyColumn("total").NotNull();
        await ApplyAsync(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_expression_spelled_differently_is_no_change()
    {
        await ExecuteAsync("""
            CREATE TABLE order_lines (id INTEGER NOT NULL, quantity INTEGER, price INTEGER,
                total BIGINT COMPUTED BY (QUANTITY*PRICE), CONSTRAINT pk_order_lines PRIMARY KEY (id))
            """);

        (await lines().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_expression_is_altered_in_place_and_converges()
    {
        await ApplyAsync(lines());
        await ExecuteAsync("INSERT INTO order_lines (id, quantity, price) VALUES (1, 3, 5)");

        var changed = lines(expression: "quantity + price");
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ScalarAsync<long>("SELECT total FROM order_lines")).ShouldBe(8L);
    }

    [Fact]
    public async Task a_changed_type_is_altered_in_place_and_converges()
    {
        await ApplyAsync(lines());

        var changed = lines(totalType: "NUMERIC(18,2)");
        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_computed_column_is_added_to_a_table_with_rows_and_dropped_again()
    {
        var plain = new Table("order_lines");
        plain.AddColumn<int>("id").AsPrimaryKey();
        plain.AddColumn<int>("quantity");
        plain.AddColumn<int>("price");
        await ApplyAsync(plain);
        await ExecuteAsync("INSERT INTO order_lines (id, quantity, price) VALUES (1, 3, 5)");

        await ApplyAsync(lines());
        (await ScalarAsync<long>("SELECT total FROM order_lines")).ShouldBe(15L);

        await ApplyAsync(plain);
        (await plain.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_back_a_changed_expression_restores_it()
    {
        await ApplyAsync(lines());

        var migration = await ApplyAsync(lines(expression: "quantity + price"));
        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await lines().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The measurement behind the Invalid delta: Firebird will not add or remove COMPUTED in place.
    /// </summary>
    [Fact]
    public async Task firebird_refuses_to_switch_a_column_between_computed_and_stored()
    {
        await ApplyAsync(lines());

        await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() =>
            ExecuteAsync("ALTER TABLE order_lines ALTER total TYPE BIGINT"));
        await Should.ThrowAsync<FirebirdSql.Data.FirebirdClient.FbException>(() =>
            ExecuteAsync("ALTER TABLE order_lines ALTER quantity COMPUTED BY (1)"));
    }

    [Fact]
    public async Task switching_a_column_between_computed_and_stored_is_refused_before_anything_runs()
    {
        await ApplyAsync(lines());

        var stored = lines();
        stored.RemoveColumn("total");
        stored.AddColumn<long>("total");
        stored.AddColumn<int>("added");

        var delta = await stored.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);

        await Should.ThrowAsync<SchemaMigrationException>(() => ApplyAsync(AutoCreate.CreateOrUpdate, stored));
        (await stored.FetchExistingAsync(theConnection))!.HasColumn("added").ShouldBeFalse();
    }

    [Fact]
    public async Task a_stored_computed_column_is_refused_before_anything_runs()
    {
        var table = lines();
        table.ModifyColumn("total").Column.ComputedColumnIsStored = true;

        await Should.ThrowAsync<NotSupportedException>(() => ApplyAsync(table));

        (await table.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
