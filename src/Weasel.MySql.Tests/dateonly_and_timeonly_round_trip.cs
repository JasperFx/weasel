using System;
using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.MySql.Tests;

/// <summary>
///     weasel#763. Neither <see cref="DateOnly" /> nor <see cref="TimeOnly" /> was in MySQL's mapping
///     table. Before weasel#758 they fell to the <c>VarChar</c> catch-all, so a date went out as
///     whatever <c>ToString()</c> produced; after it they resolved to nothing and
///     <c>ToParameterType</c> threw. <c>GetDatabaseType</c> never supported either.
/// </summary>
/// <remarks>
///     A round trip rather than a resolution assertion, because the question the issue asks is whether
///     the driver accepts the CLR type at all — which is precisely where Oracle turned out to differ.
///     The tables are named for the issue and live in the configured database rather than a schema of
///     their own, so nothing here is server-scoped and the lanes cannot collide.
/// </remarks>
[Collection("integration")]
public class dateonly_and_timeonly_round_trip : IntegrationContext
{
    private static readonly DateOnly theDate = new(2026, 10, 1);
    private static readonly TimeOnly theTime = new(13, 45, 30);

    [Fact]
    public void the_parameter_types_resolve()
    {
        MySqlProvider.Instance.ToParameterType(typeof(DateOnly)).ShouldBe(MySqlDbType.Date);
        MySqlProvider.Instance.ToParameterType(typeof(TimeOnly)).ShouldBe(MySqlDbType.Time);

        // And the nullable forms, through the shared promote-to-inner fallback.
        MySqlProvider.Instance.ToParameterType(typeof(DateOnly?)).ShouldBe(MySqlDbType.Date);
        MySqlProvider.Instance.ToParameterType(typeof(TimeOnly?)).ShouldBe(MySqlDbType.Time);
    }

    [Fact]
    public void the_column_types_resolve()
    {
        MySqlProvider.Instance.GetDatabaseType(typeof(DateOnly), EnumStorage.AsInteger).ShouldBe("DATE");
        MySqlProvider.Instance.GetDatabaseType(typeof(TimeOnly), EnumStorage.AsInteger).ShouldBe("TIME");
    }

    [Fact]
    public async Task a_date_only_round_trips_through_a_date_column()
    {
        await DropTableAsync("gh763_dates");
        await CreateTableAsync("CREATE TABLE gh763_dates (value DATE NULL)");

        await using var insert = theConnection.CreateCommand("INSERT INTO gh763_dates (value) VALUES (@p)");
        MySqlProvider.Instance.AddNamedParameter(insert, "p", theDate);
        await insert.ExecuteNonQueryAsync();

        // Read as text so the assertion is about what MySQL stored, not about how the driver
        // materialises it back.
        (await ExecuteScalarAsync<string>("SELECT CAST(value AS CHAR) FROM gh763_dates"))
            .ShouldBe("2026-10-01");
    }

    [Fact]
    public async Task a_time_only_round_trips_through_a_time_column()
    {
        await DropTableAsync("gh763_times");
        await CreateTableAsync("CREATE TABLE gh763_times (value TIME NULL)");

        await using var insert = theConnection.CreateCommand("INSERT INTO gh763_times (value) VALUES (@p)");
        MySqlProvider.Instance.AddNamedParameter(insert, "p", theTime);
        await insert.ExecuteNonQueryAsync();

        (await ExecuteScalarAsync<string>("SELECT CAST(value AS CHAR) FROM gh763_times"))
            .ShouldBe("13:45:30");
    }

    /// <summary>
    ///     The reason the mapping is worth having rather than leaving the driver to infer it: a bound
    ///     parameter has to match the stored value for a filter to find the row.
    /// </summary>
    [Fact]
    public async Task a_bound_date_only_matches_the_row_it_wrote()
    {
        await DropTableAsync("gh763_matching");
        await CreateTableAsync("CREATE TABLE gh763_matching (value DATE NULL, at TIME NULL)");

        await using var insert =
            theConnection.CreateCommand("INSERT INTO gh763_matching (value, at) VALUES (@d, @t)");
        MySqlProvider.Instance.AddNamedParameter(insert, "d", theDate);
        MySqlProvider.Instance.AddNamedParameter(insert, "t", theTime);
        await insert.ExecuteNonQueryAsync();

        await using var select = theConnection.CreateCommand(
            "SELECT COUNT(*) FROM gh763_matching WHERE value = @d AND at = @t");
        MySqlProvider.Instance.AddNamedParameter(select, "d", theDate);
        MySqlProvider.Instance.AddNamedParameter(select, "t", theTime);

        Convert.ToInt32(await select.ExecuteScalarAsync()).ShouldBe(1);
    }
}
