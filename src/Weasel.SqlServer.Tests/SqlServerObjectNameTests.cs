using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class SqlServerObjectNameTests
{
    [Fact]
    public void ShouldBeCreatedFromDBObjectName_RespectingCaseSensitivityOptions()
    {
        // Given
        const string schema = "sChEmA";
        const string name = "NaMe";
        var dbObjectName = new DbObjectName("sChEmA", "NaMe");

        //When
        var pgObjectName = SqlServerObjectName.From(dbObjectName);

        pgObjectName.Schema.ShouldBe(schema);
        pgObjectName.Name.ShouldBe(name);
        pgObjectName.QualifiedName.ShouldBe($"{schema}.{name}");
    }


    [Fact]
    public void ShouldEqualsDbObjectName_IfQualifiedNameMatches()
    {
        // Given
        const string schema = "sChEmA";
        const string name = "NaMe";
        var dbObjectName = new DbObjectName(schema, name);
        var pgObjectName = new SqlServerObjectName(schema, name);

        // When
        // Then
        (pgObjectName.Equals(dbObjectName)).ShouldBeTrue();
        (dbObjectName.GetHashCode().Equals(pgObjectName.GetHashCode())).ShouldBeTrue();
        (dbObjectName.Equals(pgObjectName)).ShouldBeTrue();
    }
}
