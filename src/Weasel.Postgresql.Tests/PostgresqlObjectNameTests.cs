using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class PostgresqlObjectNameTests
{
    [Fact]
    public void ShouldBeCreatedFromDBObjectName_RespectingCaseSensitivityOptions()
    {
        // Given
        const string schema = "sChEmA";
        const string name = "NaMe";
        var dbObjectName = new DbObjectName("sChEmA", "NaMe");

        //When
        var pgObjectName = PostgresqlObjectName.From(dbObjectName);

        var expectedQualifiedName = PostgresqlProvider.Instance.UseCaseSensitiveQualifiedNames
            ? $"\"{schema}\".\"{name}\""
            : $"{schema}.{name}";

        pgObjectName.Schema.ShouldBe(schema);
        pgObjectName.Name.ShouldBe(name);
        pgObjectName.QualifiedName.ShouldBe(expectedQualifiedName);
    }


    [Fact]
    public void ShouldEqualsDbObjectName_IfQualifiedNameMatches()
    {
        // Given
        const string schema = "sChEmA";
        const string name = "NaMe";
        var dbObjectName = new DbObjectName(schema, name);
        var pgObjectName = new PostgresqlObjectName(schema, name, $"{schema}.{name}");

        // When
        // Then
        (pgObjectName.Equals(dbObjectName)).ShouldBeTrue();
        (dbObjectName.GetHashCode().Equals(pgObjectName.GetHashCode())).ShouldBeTrue();
        (dbObjectName.Equals(pgObjectName)).ShouldBeTrue();
    }
}
