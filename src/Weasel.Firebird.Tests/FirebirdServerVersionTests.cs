using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdServerVersionTests
{
    [Theory]
    [InlineData("LI-V5.0.4.1812 Firebird 5.0/tcp (host)/P16:C", 5, 0, 4)]
    [InlineData("WI-V3.0.14.33824 Firebird 3.0", 3, 0, 14)]
    [InlineData("LI-V4.0.7.3183 Firebird 4.0", 4, 0, 7)]
    [InlineData("5.0.4", 5, 0, 4)]
    [InlineData("3.0", 3, 0, 0)]
    public void reads_the_version_from_the_server_version_or_the_engine_version(
        string text, int major, int minor, int patch)
    {
        FirebirdServerVersion.TryParse(text).ShouldBe(new FirebirdServerVersion(major, minor, patch));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Firebird")]
    public void nothing_to_read_is_no_version(string? text)
    {
        FirebirdServerVersion.TryParse(text).ShouldBeNull();
    }

    [Fact]
    public void a_closed_connection_has_no_version()
    {
        FirebirdServerVersion.Of(new FbConnection()).ShouldBeNull();
        FirebirdServerVersion.Of(null).ShouldBeNull();
    }

    [Theory]
    [InlineData(3, false, false, 31, 8, true)]
    [InlineData(4, false, true, 63, 25, false)]
    [InlineData(5, true, true, 63, 25, false)]
    public void knows_what_differs_between_3_4_and_5(
        int major, bool partialIndexes, bool firebird4Types, int maxIdentifier, int doubleFloat, bool nextFollowsStart)
    {
        var version = new FirebirdServerVersion(major, 0, 0);

        version.SupportsPartialIndexes.ShouldBe(partialIndexes);
        version.SupportsFirebird4Types.ShouldBe(firebird4Types);
        version.MaxIdentifierLength.ShouldBe(maxIdentifier);
        version.DoublePrecisionFloatThreshold.ShouldBe(doubleFloat);
        version.NextValueFollowsStartValue.ShouldBe(nextFollowsStart);
    }
}
