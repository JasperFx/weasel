using System;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
    public class TableTests
    {
        [Fact]
        public void build_table_by_name_only_puts_it_in_public()
        {
            var table = new Table("mytable");
            table.Identifier.Schema.ShouldBe("public");
            table.Identifier.Name.ShouldBe("mytable");
        }

        [Fact]
        public void add_column_by_name_and_type()
        {
            var table = new Table("mytable");
            var column = table.AddColumn("col1", "varchar");
            column.Name.ShouldBe("col1");
            column.Type.ShouldBe("varchar");
            
            table.Columns.ShouldContain(column);
        }

        [Fact]
        public void cannot_specify_column_by_enum_automatically()
        {
            Should.Throw<InvalidOperationException>(() =>
            {
                var table = new Table("mytable");
                table.AddColumn<AutoCreate>("foo");
            });
        }

        [Fact]
        public void add_type_by_dotnet_type_and_name()
        {
            var table = new Table("mytable");
            var column = table.AddColumn<int>("number");
            column
                .Type.ShouldBe(TypeMappings.GetPgType(typeof(int), EnumStorage.AsInteger));
            
            table.Columns.ShouldContain(column);
        }
    }
}