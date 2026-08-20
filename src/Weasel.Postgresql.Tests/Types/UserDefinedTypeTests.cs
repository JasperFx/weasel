using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Types;
using Xunit;

namespace Weasel.Postgresql.Tests.Types;

/// <summary>
///     PostgreSQL user-defined types — enums, domains and composites (weasel#453). One class for
///     all three, because they are one object in the catalog and Weasel does the same thing with
///     each.
/// </summary>
[Collection("udts")]
public class UserDefinedTypeTests: IntegrationContext
{
    public UserDefinedTypeTests(): base("udts")
    {
    }

    [Fact]
    public async Task an_enum_round_trips_and_reports_no_delta()
    {
        await ResetSchema();

        var type = UserDefinedType.Enum("udts.order_status", "pending", "shipped", "cancelled");
        await type.ApplyChangesAsync(theConnection);

        (await type.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await type.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_enum_does_not_report_permanent_drift()
    {
        await ResetSchema();

        var type = UserDefinedType.Enum("udts.order_status", "pending", "shipped");
        await type.ApplyChangesAsync(theConnection);

        (await type.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await type.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_enum_is_usable_as_a_column_type()
    {
        await ResetSchema();
        await UserDefinedType.Enum("udts.order_status", "pending", "shipped").ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand("create table udts.orders (id int primary key, status udts.order_status)")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand("insert into udts.orders values (1, 'shipped')").ExecuteNonQueryAsync();

        var status = await theConnection.CreateCommand("select status::text from udts.orders").ExecuteScalarAsync();
        status.ShouldBe("shipped");
    }

    /// <summary>
    ///     Changing an enum's labels is not an <c>ALTER</c> anyone can do safely — a column may
    ///     depend on the type — so it reports <see cref="SchemaPatchDifference.Invalid" /> and asks
    ///     for a human rather than dropping the type out from under a column.
    /// </summary>
    [Fact]
    public async Task changing_an_enum_reports_invalid_rather_than_dropping_it()
    {
        await ResetSchema();
        await UserDefinedType.Enum("udts.order_status", "pending", "shipped").ApplyChangesAsync(theConnection);

        var changed = UserDefinedType.Enum("udts.order_status", "pending", "shipped", "cancelled");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public async Task a_domain_round_trips_and_reports_no_delta()
    {
        await ResetSchema();

        var type = UserDefinedType.Domain("udts.positive_qty", "integer", "VALUE > 0");
        await type.ApplyChangesAsync(theConnection);

        (await type.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await type.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_domain_enforces_its_constraint()
    {
        await ResetSchema();
        await UserDefinedType.Domain("udts.positive_qty", "integer", "VALUE > 0")
            .ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand("create table udts.items (id int primary key, qty udts.positive_qty)")
            .ExecuteNonQueryAsync();

        await Should.ThrowAsync<Exception>(async () =>
            await theConnection.CreateCommand("insert into udts.items values (1, -5)").ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task a_composite_round_trips_and_reports_no_delta()
    {
        await ResetSchema();

        var type = UserDefinedType.Composite("udts.address", ("street", "varchar(100)"), ("postcode", "varchar(10)"));
        await type.ApplyChangesAsync(theConnection);

        (await type.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await type.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_types_with_it()
    {
        await ResetSchema();

        var type = UserDefinedType.Enum("udts.order_status", "pending");
        await type.ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await type.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
