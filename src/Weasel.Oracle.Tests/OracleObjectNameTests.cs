using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

public class OracleObjectNameTests
{
    [Fact]
    public void parses_qualified_name()
    {
        var name = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.PEOPLE");
        name.Schema.ShouldBe("WEASEL");
        name.Name.ShouldBe("PEOPLE");
    }

    [Fact]
    public void parses_unqualified_name_uses_default_schema()
    {
        var name = DbObjectName.Parse(OracleProvider.Instance, "PEOPLE");
        name.Schema.ShouldBe("WEASEL");
        name.Name.ShouldBe("PEOPLE");
    }

    [Fact]
    public void equality_is_case_insensitive()
    {
        var name1 = new OracleObjectName("WEASEL", "PEOPLE");
        var name2 = new OracleObjectName("weasel", "people");

        name1.Equals(name2).ShouldBeTrue();
    }

    [Fact]
    public void from_db_object_name()
    {
        var dbName = new DbObjectName("WEASEL", "PEOPLE");
        var oracleName = OracleObjectName.From(dbName);

        oracleName.Schema.ShouldBe("WEASEL");
        oracleName.Name.ShouldBe("PEOPLE");
    }
}
