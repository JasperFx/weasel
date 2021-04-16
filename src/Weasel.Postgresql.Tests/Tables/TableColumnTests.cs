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
            var states = new Table("states");

            states.AddColumn("col1", "varchar").AllowNulls();
            var column = states.Columns.Single();

            column.ColumnChecks.Single()
                .ShouldBeOfType<AllowNulls>();
        }

        [Fact]
        public void set_not_null()
        {
            var states = new Table("states");

            states.AddColumn("col1", "varchar").NotNull();
            var column = states.Columns.Single();

            column.ColumnChecks.Single()
                .ShouldBeOfType<NotNull>();
        }

        [Fact]
        public void allow_null_then_not_null()
        {
            var states = new Table("states");

            states.AddColumn("col1", "varchar").NotNull();
            var column = states.Columns.Single();
            
            // last one wins
            column.ColumnChecks.Single()
                .ShouldBeOfType<NotNull>();
        }

        [Fact]
        public void not_null_then_allow_null()
        {
            var states = new Table("states");

            states.AddColumn("col1", "varchar").NotNull().AllowNulls();
            var column = states.Columns.Single();
            
            column.ColumnChecks.Single()
                .ShouldBeOfType<AllowNulls>();
        }

        [Fact]
        public void determine_the_directive_with_basic_options()
        {
            var states = new Table("states");

            var expression = states.AddColumn("col1", "varchar");
            var column = states.Columns.Single();
            
            // nothing
            column.CheckDeclarations()
                .ShouldBeEmpty();

            expression.AllowNulls();
            column.CheckDeclarations().ShouldBe("NULL");

            expression.NotNull();
            column.CheckDeclarations().ShouldBe("NOT NULL");
        }

        [Fact]
        public void column_should_build_the_directive_for_primary_key_if_it_is_a_one_column_pk()
        {
            var table = new Table("mytable");
            table
                .AddColumn("id", "int")
                .NotNull()
                .AsPrimaryKey();

            var column = table.Columns.Single();

            table.PrimaryKeyConstraintName = "pk_mytable";
            
            column.CheckDeclarations()
                .ShouldBe("CONSTRAINT pk_mytable PRIMARY KEY");


        }
    }
}