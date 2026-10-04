using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     Firebird stores a type, not the words it was declared with, so a model written with a synonym
///     has to match the column the server created from it -- or every migration re-issues an ALTER
///     that changes nothing (weasel#646's lesson, from MySQL).
/// </summary>
public class detecting_column_type_synonyms: IntegrationContext
{
    [Theory]
    [InlineData("INT")]
    [InlineData("INTEGER")]
    [InlineData("DEC(10,2)")]
    [InlineData("NUMERIC(18,4)")]
    [InlineData("NUMERIC(9)")]
    [InlineData("NUMERIC")]
    [InlineData("DECIMAL(4,2)")]
    [InlineData("REAL")]
    [InlineData("DOUBLE PRECISION")]
    [InlineData("CHARACTER(5)")]
    [InlineData("CHAR")]
    [InlineData("CHARACTER VARYING(40)")]
    [InlineData("CHAR VARYING(40)")]
    [InlineData("NCHAR(4)")]
    [InlineData("NATIONAL CHARACTER VARYING(4)")]
    [InlineData("VARCHAR(40) CHARACTER SET UTF8")]
    [InlineData("VARCHAR(40) CHARACTER SET NONE")]
    [InlineData("VARCHAR(40) COLLATE UNICODE_CI")]
    [InlineData("VARCHAR(40) CHARACTER SET UTF8 COLLATE UNICODE_CI_AI")]
    [InlineData("CHAR(16) CHARACTER SET OCTETS")]
    [InlineData("BLOB")]
    [InlineData("BLOB SUB_TYPE 0")]
    [InlineData("BLOB SUB_TYPE TEXT")]
    [InlineData("BLOB SUB_TYPE 1 SEGMENT SIZE 80")]
    [InlineData("BOOLEAN")]
    [InlineData("TIMESTAMP")]
    [InlineData("TIME")]
    [InlineData("DATE")]
    [InlineData("FLOAT")]
    [InlineData("FLOAT(10)")]
    [InlineData("FLOAT(30)")]
    public async Task a_column_declared_with_a_synonym_reads_back_as_no_change(string type)
    {
        var table = new Table("typed");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("value_column", type);

        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None,
            $"{type} read back as {delta.Columns.Different.SingleOrDefault()?.Actual.Type}");
    }

    /// <summary>
    ///     The types Firebird 4 introduced. Firebird 3 refuses them outright.
    /// </summary>
    [Theory]
    [InlineData("INT128")]
    [InlineData("DECFLOAT")]
    [InlineData("DECFLOAT(16)")]
    [InlineData("NUMERIC(38,4)")]
    [InlineData("BINARY(16)")]
    [InlineData("VARBINARY(8)")]
    [InlineData("BINARY VARYING(8)")]
    [InlineData("TIME WITH TIME ZONE")]
    [InlineData("TIMESTAMP WITH TIME ZONE")]
    [InlineData("TIMESTAMP WITHOUT TIME ZONE")]
    public async Task a_firebird_4_type_reads_back_as_no_change(string type)
    {
        if (!ServerVersion.SupportsFirebird4Types)
        {
            Assert.Skip($"{type} is a Firebird 4 type, and this server is Firebird {ServerVersion}");
        }

        await a_column_declared_with_a_synonym_reads_back_as_no_change(type);
    }

    /// <summary>
    ///     A column created outside Weasel, spelled one way, and a model spelled another.
    /// </summary>
    [Theory]
    [InlineData("INT", "INTEGER")]
    [InlineData("CHARACTER VARYING(40)", "VARCHAR(40)")]
    [InlineData("NUMERIC(9,0)", "NUMERIC")]
    [InlineData("BLOB SUB_TYPE 1", "BLOB SUB_TYPE TEXT")]
    [InlineData("DOUBLE PRECISION", "FLOAT(53)")]
    [InlineData("CHAR(16) CHARACTER SET OCTETS", "CHAR(16) CHARACTER SET OCTETS")]
    public async Task a_model_spelled_differently_from_the_ddl_is_no_change(string created, string modelled)
    {
        await ExecuteAsync($"CREATE TABLE typed (id INTEGER NOT NULL, value_column {created}, CONSTRAINT pk_typed PRIMARY KEY (id))");

        var table = new Table("typed");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("value_column", modelled);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_different_type_is_still_a_difference()
    {
        await ExecuteAsync("CREATE TABLE typed (id INTEGER NOT NULL, value_column INTEGER, CONSTRAINT pk_typed PRIMARY KEY (id))");

        var table = new Table("typed");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("value_column", "BIGINT");

        var delta = await table.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Different.Single().Actual.Type.ShouldBe("INTEGER");

        await ApplyAsync(table);
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     weasel#644: a character set the model states is compared.
    /// </summary>
    [Fact]
    public async Task a_stated_character_set_is_compared()
    {
        await ExecuteAsync("CREATE TABLE typed (id INTEGER NOT NULL, value_column VARCHAR(20) CHARACTER SET NONE, CONSTRAINT pk_typed PRIMARY KEY (id))");

        var table = new Table("typed");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("value_column", "VARCHAR(20) CHARACTER SET UTF8");

        (await table.FindDeltaAsync(theConnection)).Columns.Different.Count.ShouldBe(1);
    }

    [Fact]
    public async Task the_clr_type_map_reads_back_as_no_change()
    {
        var table = new Table("mapped");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("text_value");
        table.AddColumn<bool>("bool_value");
        table.AddColumn<byte>("byte_value");
        table.AddColumn<short>("short_value");
        table.AddColumn<long>("long_value");
        table.AddColumn<decimal>("decimal_value");
        table.AddColumn<double>("double_value");
        table.AddColumn<float>("float_value");
        table.AddColumn<DateTime>("datetime_value");
        table.AddColumn<DateOnly>("date_value");
        table.AddColumn<TimeOnly>("time_value");
        table.AddColumn<TimeSpan>("timespan_value");
        table.AddColumn<Guid>("guid_value");
        table.AddColumn<byte[]>("bytes_value");
        table.AddColumn("json_value", new FirebirdMigrator().DefaultJsonColumnType);

        if (ServerVersion.SupportsFirebird4Types)
        {
            table.AddColumn<DateTimeOffset>("offset_value");
        }

        await CreateSchemaObjectInDatabase(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
