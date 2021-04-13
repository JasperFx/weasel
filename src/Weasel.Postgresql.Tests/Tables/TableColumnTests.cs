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
    }
}