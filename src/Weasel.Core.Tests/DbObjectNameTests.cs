using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

public class DbObjectNameTests
{
    public class StubProvider: IDatabaseProvider
    {
        public string DefaultDatabaseSchemaName => "stub";

        public DbObjectName Parse(string schemaName, string objectName) =>
            new DbObjectName(schemaName, objectName);

        public string AddApplicationNameToConnectionString(string connectionString, string applicationName)
        {
            throw new NotImplementedException();
        }
    }

    private readonly IDatabaseProvider theProvider = new StubProvider();

    [Fact]
    public void default_schema_name_is_public()
    {
        var name = new DbObjectName("public", "foo");
        name.Schema.ShouldBe("public");
    }

    [Fact]
    public void qualified_name()
    {
        var name = new DbObjectName("public", "foo");
        name.QualifiedName.ShouldBe("public.foo");
    }

    [Fact]
    public void to_string_is_qualified_name()
    {
        var name = new DbObjectName("public", "foo");
        name.ToString().ShouldBe("public.foo");
    }

    [Fact]
    public void to_temp_copy_table_appends__temp()
    {
        var name = new DbObjectName("public", "foo");
        var tempCopy = name.ToTempCopyTable();
        tempCopy.QualifiedName.ShouldBe("public.foo_temp");
    }

    [Fact]
    public void parse_qualified_name_with_schema()
    {
        var name = DbObjectName.Parse(theProvider, "other.foo");
        name.Schema.ShouldBe("other");
        name.Name.ShouldBe("foo");
    }

    [Fact]
    public void parse_qualified_name_without_schema()
    {
        var name = DbObjectName.Parse(theProvider, "foo");
        name.Schema.ShouldBe(theProvider.DefaultDatabaseSchemaName);
        name.Name.ShouldBe("foo");
    }
}
