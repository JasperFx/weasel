using System.Linq;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
    public class TableColumnTests
    {
        [Fact]
        public void set_as_null()
        {
            var column = new TableColumn("col1", "varchar");
            column.AllowNulls();

            column.ColumnChecks.Single()
                .ShouldBeOfType<AllowNulls>();
        }

        [Fact]
        public void set_not_null()
        {
            var column = new TableColumn("col1", "varchar");
            column.NotNull();

            column.ColumnChecks.Single()
                .ShouldBeOfType<NotNull>();
        }

        [Fact]
        public void allow_null_then_not_null()
        {
            var column = new TableColumn("col1", "varchar");
            column.AllowNulls().NotNull();
            
            // last one wins
            column.ColumnChecks.Single()
                .ShouldBeOfType<NotNull>();
        }

        [Fact]
        public void not_null_then_allow_null()
        {
            var column = new TableColumn("col1", "varchar");
            column.NotNull().AllowNulls();
            
            column.ColumnChecks.Single()
                .ShouldBeOfType<AllowNulls>();
        }

        [Fact]
        public void is_primary_key_mechanics()
        {
            var column = new TableColumn("col1", "int");
            column.IsPrimaryKey.ShouldBeFalse();

            column.AsPrimaryKey();
            
            column.IsPrimaryKey.ShouldBeTrue();
        }

        [Fact]
        public void determine_the_directive_with_basic_options()
        {
            var column = new TableColumn("col1", "int");
            // nothing
            column.CheckDeclarations()
                .ShouldBeEmpty();

            column.AllowNulls();
            column.CheckDeclarations().ShouldBe("NULL");

            column.NotNull();
            column.CheckDeclarations().ShouldBe("NOT NULL");
        }

        [Fact]
        public void column_should_build_the_directive_for_primary_key_if_it_is_a_one_column_pk()
        {
            var table = new Table("mytable");
            var column = table
                .AddColumn("id", "int")
                .NotNull()
                .AsPrimaryKey();

            table.PrimaryKeyConstraintName = "pk_mytable";
            
            column.CheckDeclarations()
                .ShouldBe("CONSTRAINT pk_mytable PRIMARY KEY");


        }
    }
}