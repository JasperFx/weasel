using System;
using System.IO;
using System.Linq;
using Baseline;
using Shouldly;
using Weasel.Core;
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
        public void add_column_by_name_and_type()
        {
            var table = new Table("mytable");
            table.AddColumn("col1", "varchar");
            var column = table.Columns.Single();
            
            column.Name.ShouldBe("col1");
            column.Type.ShouldBe("varchar");
            
            table.Columns.ShouldContain(column);
        }

        [Fact]
        public void has_column()
        {
            var table = new Table("mytable");
            table.AddColumn("col1", "varchar");
            
            table.HasColumn("col1").ShouldBeTrue();
            table.HasColumn("doesnotexist").ShouldBeFalse();
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
            table.AddColumn("col1", "varchar");
            var column = table.Columns.Single();
            
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
            table.AddColumn<int>("number");
            var column = table.Columns.Single();
            column
                .Type.ShouldBe(TypeMappings.Instance.GetDatabaseType(typeof(int), EnumStorage.AsInteger));
            
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
            table.WriteCreateStatement(rules, writer);

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
            table.WriteCreateStatement(rules, writer);

            var ddl = writer.ToString();
            
            _output.WriteLine(ddl);
            
            var lines = ddl.ReadLines().ToArray();
            
            lines.ShouldContain("CREATE TABLE IF NOT EXISTS public.people (");
        }

        [Fact]
        public void add_foreign_key_to_table_with_fluent_interface()
        {
            var states = new Table("states");
            states.AddColumn<int>("id").AsPrimaryKey();

            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

            var fk = table.ForeignKeys.Single();
            
            fk.Name.ShouldBe("fkey_people_state_id");
        }
        
        
        [Fact]
        public void add_foreign_key_to__external_table_by_name_with_fluent_interface()
        {
            var states = new Table("states");
            states.AddColumn<int>("id").AsPrimaryKey();

            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

            var fk = table.ForeignKeys.Single();
            
            fk.Name.ShouldBe("fkey_people_state_id");
        }
        
        
        [Fact]
        public void add_index_to_table_with_fluent_interface()
        {

            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").AddIndex();

            var index = table.Indexes.Single().ShouldBeOfType<IndexDefinition>();
            
            index.Name.ShouldBe("idx_people_state_id");
            index.Columns.Single().ShouldBe("state_id");
        }
        
                
        
        [Fact]
        public void add_index_to_table_with_fluent_interface_with_customization()
        {

            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").AddIndex(i => i.Method = IndexMethod.hash);

            var index = table.Indexes.Single().ShouldBeOfType<IndexDefinition>();
            
            index.Name.ShouldBe("idx_people_state_id");
            index.Columns.Single().ShouldBe("state_id");
            index.Method.ShouldBe(IndexMethod.hash);
        }

        [Fact]
        public void default_primary_key_name()
        {
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").AddIndex(i => i.Method = IndexMethod.hash);

            table.PrimaryKeyName.ShouldBe("pkey_people_id");
        }

        [Fact]
        public void multi_column_primary_key()
        {
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name").AsPrimaryKey();
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").AddIndex(i => i.Method = IndexMethod.hash);

            table.PrimaryKeyName.ShouldBe("pkey_people_id_first_name");
        }
        
        [Fact]
        public void override_primary_key_name()
        {
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name").AsPrimaryKey();
            table.AddColumn<string>("last_name");
            table.AddColumn<int>("state_id").AddIndex(i => i.Method = IndexMethod.hash);

            table.PrimaryKeyName = "pk_people";
            table.PrimaryKeyName.ShouldBe("pk_people");
        }
        
        
        [Fact]
        public void is_primary_key_mechanics()
        {
            var states = new Table("states");
            states.AddColumn<int>("id").AsPrimaryKey();
            states.AddColumn<string>("abbreviation");
            
            states.Columns.First().IsPrimaryKey.ShouldBeTrue();
            states.Columns.Last().IsPrimaryKey.ShouldBeFalse();
        }

        [Fact]
        public void add_serial_as_default_value()
        {
            var states = new Table("states");
            states.AddColumn<int>("id").AsPrimaryKey().Serial();


            var sql = states.ToBasicCreateTableSql();
            
            sql.ShouldContain("id SERIAL");
        }

    }
}