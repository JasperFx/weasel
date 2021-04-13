using System;
using System.IO;
using System.Linq;
using Baseline;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;
using Xunit.Abstractions;

namespace Weasel.Postgresql.Tests.Tables
{
    public class TableTests
    {
        private readonly ITestOutputHelper _output;

        public TableTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void build_table_by_name_only_puts_it_in_public()
        {
            var table = new Table("mytable");
            table.Identifier.Schema.ShouldBe("public");
            table.Identifier.Name.ShouldBe("mytable");
        }

        [Fact]
        public void default_primary_key_name_is_derived_from_table_name()
        {
            var table = new Table("mytable");
            table.PrimaryKeyConstraintName.ShouldBe("pk_mytable");
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
        public void add_column_directly_sets_the_parent()
        {
            var table = new Table("mytable");
            var column = new TableColumn("col1", "varchar");

            table.AddColumn(column);
            
            table.Columns.ShouldContain(column);
            column.Parent.ShouldBe(table);
        }

        [Fact]
        public void set_the_parent_on_add_column()
        {
            var table = new Table("mytable");
            var column = table.AddColumn("col1", "varchar");

            column.Parent.ShouldBe(table);
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

        [Fact]
        public void smoke_test_writing_table_code_with_columns()
        {
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            var rules = new DdlRules
            {
                TableCreation = CreationStyle.DropThenCreate
            };

            var writer = new StringWriter();
            table.Write(rules, writer);

            var ddl = writer.ToString();
            
            _output.WriteLine(ddl);
            
            var lines = ddl.ReadLines().ToArray();
            
            lines.ShouldContain("DROP TABLE IF EXISTS public.people CASCADE;");
            lines.ShouldContain("CREATE TABLE public.people (");

        }

        [Fact]
        public void write_table_with_if_exists_semantics()
        {
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            var rules = new DdlRules
            {
                TableCreation = CreationStyle.CreateIfNotExists
            };

            var writer = new StringWriter();
            table.Write(rules, writer);

            var ddl = writer.ToString();
            
            _output.WriteLine(ddl);
            
            var lines = ddl.ReadLines().ToArray();
            
            lines.ShouldContain("CREATE TABLE IF NOT EXISTS public.people (");
        }
    }
}