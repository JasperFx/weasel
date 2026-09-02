using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

public class CharacterColumnLengthTests
{
    [Theory]
    [InlineData("varchar(255)", 255)]
    [InlineData("VARCHAR(1000)", 1000)]
    [InlineData("nvarchar(50)", 50)]
    [InlineData("char(1)", 1)]
    [InlineData("varbinary(16)", 16)]
    [InlineData("VARCHAR2(500)", 500)]
    // Oracle spells the semantics out; the number is still the length.
    [InlineData("VARCHAR2(100 CHAR)", 100)]
    [InlineData("VARCHAR2(100 BYTE)", 100)]
    [InlineData("varchar(max)", CharacterColumnLength.Unbounded)]
    public void reads_the_declared_length(string type, int expected)
    {
        CharacterColumnLength.TryParse(type).ShouldBe(expected);
    }

    [Theory]
    // No length declared at all.
    [InlineData("varchar")]
    [InlineData("text")]
    [InlineData(null)]
    // The parenthesised part is not a character length, and comparing it is what produces drift on
    // tables nobody touched -- a MySQL INT display width the catalog invented, a decimal precision and
    // scale, a fractional-seconds precision.
    [InlineData("int(11)")]
    [InlineData("decimal(18,2)")]
    [InlineData("datetime(6)")]
    [InlineData("NUMBER(10)")]
    [InlineData("timestamp(3) with time zone")]
    public void reads_nothing_from_a_type_whose_size_is_not_a_character_length(string? type)
    {
        CharacterColumnLength.TryParse(type).ShouldBeNull();
    }

    [Fact]
    public void a_widened_character_column_differs()
    {
        CharacterColumnLength.Differ("varchar(1000)", "varchar(255)").ShouldBeTrue();
        CharacterColumnLength.Differ("VARCHAR2(1000)", "varchar2(500)").ShouldBeTrue();
    }

    [Fact]
    public void the_same_length_does_not_differ()
    {
        CharacterColumnLength.Differ("varchar(255)", "VARCHAR(255)").ShouldBeFalse();
        CharacterColumnLength.Differ("varchar(max)", "VARCHAR(MAX)").ShouldBeFalse();
    }

    [Fact]
    public void an_unbounded_column_differs_from_a_bounded_one()
    {
        CharacterColumnLength.Differ("varchar(max)", "varchar(500)").ShouldBeTrue();
    }

    [Fact]
    public void a_length_on_only_one_side_is_not_a_difference()
    {
        // "Cannot tell" must never report drift: a model that declares a bare type keeps comparing the
        // way it always has, against whatever the catalog reports.
        CharacterColumnLength.Differ("varchar", "varchar(255)").ShouldBeFalse();
        CharacterColumnLength.Differ("varchar(255)", "varchar").ShouldBeFalse();
    }

    [Fact]
    public void sizes_that_are_not_character_lengths_are_never_a_difference()
    {
        CharacterColumnLength.Differ("int(11)", "int").ShouldBeFalse();
        CharacterColumnLength.Differ("decimal(18,2)", "decimal(10,4)").ShouldBeFalse();
        CharacterColumnLength.Differ("datetime(6)", "datetime(3)").ShouldBeFalse();
    }
}
