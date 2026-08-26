using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     information_schema.columns reports length in characters and has no is_identity, so a column
///     read back through it lost its precision, its scale and the fact that it was an IDENTITY.
///     Everything that regenerates DDL from a fetched table -- rollback above all -- then emitted a
///     narrower column than the one in the database.
/// </summary>
public class column_introspection_fidelity: IntegrationContext
{
    public column_introspection_fidelity(): base("fidelity")
    {
    }

    [Theory]
    [InlineData("decimal(18,2)", "decimal(18,2)")]
    [InlineData("numeric(9,4)", "numeric(9,4)")]
    [InlineData("varchar(50)", "varchar(50)")]
    [InlineData("varchar(max)", "varchar(max)")]
    [InlineData("nvarchar(50)", "nvarchar(50)")]
    [InlineData("nvarchar(max)", "nvarchar(max)")]
    [InlineData("nchar(10)", "nchar(10)")]
    [InlineData("binary(16)", "binary(16)")]
    [InlineData("varbinary(max)", "varbinary(max)")]
    [InlineData("datetime2(3)", "datetime2(3)")]
    [InlineData("time(4)", "time(4)")]
    [InlineData("datetimeoffset(7)", "datetimeoffset(7)")]
    [InlineData("int", "int")]
    [InlineData("bit", "bit")]
    [InlineData("uniqueidentifier", "uniqueidentifier")]
    public async Task a_store_type_round_trips(string declared, string expected)
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                $"create table fidelity.types (id int not null constraint pk_types primary key, value {declared} null)")
            .ExecuteNonQueryAsync();

        var existing = await new Table("fidelity.types").FetchExistingAsync(theConnection);

        existing!.ColumnFor("value")!.Type.ShouldBe(expected);
    }

    [Fact]
    public async Task an_identity_column_is_read_back_as_an_identity()
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                "create table fidelity.ident (id int identity(1,1) not null constraint pk_ident primary key, name varchar(20) null)")
            .ExecuteNonQueryAsync();

        var existing = await new Table("fidelity.ident").FetchExistingAsync(theConnection);

        existing!.ColumnFor("id")!.IsAutoNumber.ShouldBeTrue();
        existing.ColumnFor("name")!.IsAutoNumber.ShouldBeFalse();
    }

    [Fact]
    public async Task nullability_and_defaults_still_come_back()
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                "create table fidelity.defaults (id int not null constraint pk_defaults primary key, note varchar(20) null, count int not null constraint df_count default 0)")
            .ExecuteNonQueryAsync();

        var existing = await new Table("fidelity.defaults").FetchExistingAsync(theConnection);

        existing!.ColumnFor("note")!.AllowNulls.ShouldBeTrue();
        existing.ColumnFor("count")!.AllowNulls.ShouldBeFalse();
        existing.ColumnFor("count")!.DefaultExpression.ShouldBe("((0))");
    }

    [Fact]
    public async Task columns_keep_their_declared_order()
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                "create table fidelity.ordered (zebra int not null constraint pk_ordered primary key, apple int null, mango int null)")
            .ExecuteNonQueryAsync();

        var existing = await new Table("fidelity.ordered").FetchExistingAsync(theConnection);

        existing!.Columns.Select(x => x.Name).ShouldBe(["zebra", "apple", "mango"]);
    }
}
