using System;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     weasel#763. The issue asked for <c>DateOnly</c> -> <c>OracleDbType.Date</c> and something for
///     <c>TimeOnly</c>, each verified by a bind-and-round-trip rather than a resolution assertion,
///     "because the question is whether the driver accepts the CLR type at all". It does not — for
///     either type, against any candidate <see cref="OracleDbType" />. So Oracle gets the two column
///     types and an explicit refusal on the parameter, and this class is the measurement.
/// </summary>
/// <remarks>
///     What was tried, against ODP.NET 23.1.0.0 and Oracle Free 23, with <c>OracleDbType</c> assigned
///     <em>before</em> the value — assigning it after throws from the value setter alone, which is a
///     red herring, since it is the bind that cannot be done:
///     <list type="table">
///         <item><term>DateOnly as Date</term><description>InvalidCastException: DateOnly is not
///         IConvertible, and ODP.NET binds a date through Convert.ToDateTime</description></item>
///         <item><term>DateOnly/TimeOnly as TimeStamp</term><description>ORA-50028: Invalid parameter
///         binding</description></item>
///         <item><term>TimeOnly as IntervalDS</term><description>ArgumentException: Value does not
///         fall within the expected range</description></item>
///         <item><term>TimeOnly as Varchar2</term><description>executes, and writes "1:45 PM" — the
///         current culture's ToString(), which is the pre-#758 defect rather than a mapping</description></item>
///     </list>
/// </remarks>
[Collection("integration")]
public class dateonly_and_timeonly_are_refused : IntegrationContext
{
    private static readonly DateOnly theDate = new(2026, 10, 1);
    private static readonly TimeOnly theTime = new(13, 45, 30);

    public dateonly_and_timeonly_are_refused() : base("gh763")
    {
    }

    [Theory]
    [InlineData(typeof(DateOnly))]
    [InlineData(typeof(TimeOnly))]
    public void the_parameter_type_is_refused_with_an_explanation(Type type)
    {
        var ex = Should.Throw<NotSupportedException>(() => OracleProvider.Instance.ToParameterType(type));

        ex.Message.ShouldContain($"Oracle cannot bind a {type.Name} parameter");
        ex.Message.ShouldContain("Convert the value first");
        ex.Message.ShouldContain("weasel#763");
    }

    /// <summary>
    ///     The nullable forms go through the same refusal rather than reaching the driver by a side
    ///     door — the nullable branch resolves the inner type through <c>ToParameterType</c>.
    /// </summary>
    [Theory]
    [InlineData(typeof(DateOnly?))]
    [InlineData(typeof(TimeOnly?))]
    public void the_nullable_forms_are_refused_too(Type type)
    {
        Should.Throw<NotSupportedException>(() => OracleProvider.Instance.ToParameterType(type));
    }

    /// <summary>
    ///     The column types are still mapped, which is the half of the issue that can be honoured: a
    ///     schema can declare one, and the value is bound as what it converts to.
    /// </summary>
    [Fact]
    public void the_column_types_are_mapped()
    {
        OracleProvider.Instance.GetDatabaseType(typeof(DateOnly), EnumStorage.AsInteger)
            .ShouldBe("DATE");
        OracleProvider.Instance.GetDatabaseType(typeof(TimeOnly), EnumStorage.AsInteger)
            .ShouldBe("INTERVAL DAY TO SECOND");
    }

    /// <summary>
    ///     Deferring to the driver is not an alternative to refusing: an untyped OracleParameter
    ///     rejects the value outright, so there would be nothing for ODP.NET to infer from. Measured
    ///     here so the refusal above is not taken on trust.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void an_untyped_oracle_parameter_will_not_even_hold_the_value(bool date)
    {
        var parameter = new OracleParameter { ParameterName = "p" };

        Should.Throw<ArgumentException>(() => parameter.Value = date ? theDate : (object)theTime);
    }

    /// <summary>
    ///     And the conversion the refusal tells the caller to make does work, against the column types
    ///     Weasel now generates for the two CLR types.
    /// </summary>
    [Fact]
    public async Task the_documented_conversion_round_trips()
    {
        await ResetSchema();

        await Execute("CREATE TABLE GH763.MOMENTS (THE_DATE DATE, THE_TIME INTERVAL DAY TO SECOND)");

        await using var insert =
            theConnection.CreateCommand("INSERT INTO GH763.MOMENTS (THE_DATE, THE_TIME) VALUES (:d, :t)");
        OracleProvider.Instance.AddNamedParameter(insert, "d", theDate.ToDateTime(TimeOnly.MinValue));
        OracleProvider.Instance.AddNamedParameter(insert, "t", theTime.ToTimeSpan());
        await insert.ExecuteNonQueryAsync();

        await using var read = theConnection.CreateCommand(
            "SELECT TO_CHAR(THE_DATE, 'YYYY-MM-DD'), TO_CHAR(THE_TIME) FROM GH763.MOMENTS");
        await using var reader = await read.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        reader.GetString(0).ShouldBe("2026-10-01");
        reader.GetString(1).ShouldContain("13:45:30");
    }

    private async Task Execute(string sql)
    {
        await using var cmd = theConnection.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }
}
