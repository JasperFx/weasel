using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public class DbObjectNameTests
    {
        [Fact]
        public void default_schema_name_is_public()
        {
            var name = new DbObjectName("foo");
            name.Schema.ShouldBe("public");
        }

        [Fact]
        public void qualified_name()
        {
            var name = new DbObjectName("foo");
            name.QualifiedName.ShouldBe("public.foo");
        }

        [Fact]
        public void to_string_is_qualified_name()
        {
            var name = new DbObjectName("foo");
            name.ToString().ShouldBe("public.foo");
        }

        [Fact]
        public void to_temp_copy_table_appends__temp()
        {
            var name = new DbObjectName("foo");
            var tempCopy = name.ToTempCopyTable();
            tempCopy.QualifiedName.ShouldBe("public.foo_temp");
        }

        [Fact]
        public void parse_qualified_name_with_schema()
        {
            var name = DbObjectName.Parse("other.foo");
            name.Schema.ShouldBe("other");
            name.Name.ShouldBe("foo");
        }

        [Fact]
        public void parse_qualified_name_without_schema()
        {
            var name = DbObjectName.Parse("foo");
            name.Schema.ShouldBe("public");
            name.Name.ShouldBe("foo");
        }
    }
}