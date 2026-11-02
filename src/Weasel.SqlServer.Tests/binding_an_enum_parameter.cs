using System;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#761, live. The resolution assertions live in <c>Weasel.Core.Tests</c>; this is the half
///     that proves the old answer was not merely slow. Binding an enum as <c>sql_variant</c> — SQL
///     Server's catch-all, and what every enum resolved to before — <em>throws</em>, against an
///     <c>int</c> column and a <c>varchar</c> one alike. So there was no behaviour to preserve here,
///     which is what made the choice in #761 a straightforward one on this provider.
/// </summary>
[Collection("integration")]
public class binding_an_enum_parameter : IntegrationContext
{
    public binding_an_enum_parameter() : base("gh761")
    {
    }

    public enum Flavor
    {
        Vanilla = 0,
        Chocolate = 1,
        Strawberry = 2
    }

    private async Task Setup()
    {
        await ResetSchema();

        await using var create = theConnection.CreateCommand(
            "CREATE TABLE gh761.flavors (as_integer int NULL, as_string varchar(50) NULL)");
        await create.ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     The default: an enum binds as its underlying integer, which matches the column
    ///     <c>GetDatabaseType</c> generates under <see cref="EnumStorage.AsInteger" />.
    /// </summary>
    [Fact]
    public async Task binds_as_an_integer_by_default()
    {
        await Setup();

        await using var insert =
            theConnection.CreateCommand("INSERT INTO gh761.flavors (as_integer) VALUES (@p)");
        SqlServerProvider.Instance.AddNamedParameter(insert, "p", (object)Flavor.Strawberry);
        await insert.ExecuteNonQueryAsync();

        await using var read = theConnection.CreateCommand("SELECT as_integer FROM gh761.flavors");
        Convert.ToInt32(await read.ExecuteScalarAsync()).ShouldBe(2);
    }

    /// <summary>
    ///     And told <see cref="EnumStorage.AsString" />, it binds the name — against the
    ///     <c>varchar</c> column that storage generates. This is the case that had no answer at all
    ///     before: the parameter seam could not be told, so an <c>AsString</c> consumer on SQL Server
    ///     had nothing that worked.
    /// </summary>
    [Fact]
    public async Task binds_as_the_name_when_told_as_string()
    {
        await Setup();

        await using var insert =
            theConnection.CreateCommand("INSERT INTO gh761.flavors (as_string) VALUES (@p)");
        SqlServerProvider.Instance.AddNamedParameter(insert, "p", Flavor.Strawberry,
            SqlServerProvider.Instance.ToParameterType(typeof(Flavor), EnumStorage.AsString));
        await insert.ExecuteNonQueryAsync();

        await using var read = theConnection.CreateCommand("SELECT as_string FROM gh761.flavors");
        (await read.ExecuteScalarAsync()).ShouldBe("Strawberry");
    }

    /// <summary>
    ///     The old answer, reproduced directly against the driver so the premise is on the record
    ///     rather than in a commit message. Not a Weasel call — <c>SqlDbType.Variant</c> is simply
    ///     what every enum used to resolve to.
    /// </summary>
    [Theory]
    [InlineData("as_integer")]
    [InlineData("as_string")]
    public async Task sql_variant_could_not_bind_an_enum_at_all(string column)
    {
        await Setup();

        await using var insert =
            theConnection.CreateCommand($"INSERT INTO gh761.flavors ({column}) VALUES (@p)");
        var parameter = insert.CreateParameter();
        parameter.ParameterName = "@p";
        parameter.Value = Flavor.Strawberry;
        parameter.SqlDbType = System.Data.SqlDbType.Variant;
        insert.Parameters.Add(parameter);

        var ex = await Should.ThrowAsync<SqlException>(() => insert.ExecuteNonQueryAsync());
        ex.Message.ShouldContain("Implicit conversion from data type sql_variant");
    }

    /// <summary>
    ///     An array defers now rather than binding as the <c>Variant</c> catch-all, so Weasel leaves
    ///     the parameter untyped for the driver to rule on. SqlClient then says what it thinks of an
    ///     <c>int[]</c> in its own words — which is the point: Weasel no longer claims a type it does
    ///     not have, and the failure names the real problem instead of the conversion it caused.
    /// </summary>
    /// <remarks>
    ///     Reading <c>SqlDbType</c> off the parameter is what forces SqlClient's inference, and it
    ///     throwing is the assertion: had Weasel set a type, the getter would simply return it.
    /// </remarks>
    [Fact]
    public void an_array_is_left_for_the_driver()
    {
        SqlServerProvider.Instance.TryGetDbType(typeof(int[])).ShouldBeNull();

        var command = new SqlCommand();
        SqlServerProvider.Instance.AddNamedParameter(command, "p", new[] { 1, 2, 3 });
        var parameter = (SqlParameter)command.Parameters[0];

        Should.Throw<ArgumentException>(() => _ = parameter.SqlDbType)
            .Message.ShouldContain("No mapping exists from object type System.Int32[]");
    }
}
