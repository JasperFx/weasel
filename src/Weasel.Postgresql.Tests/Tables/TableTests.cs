using JasperFx;
using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;
using Xunit.Abstractions;

namespace Weasel.Postgresql.Tests.Tables;

using static PostgresqlProvider;

public class TableTests
{
    private readonly ITestOutputHelper _output;

    public TableTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void move_to_different_schema()
    {
        var table = new Table("mytable");
        table.MoveToSchema("other");
        table.Identifier.Schema.ShouldBe("other");
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
            .Type.ShouldBe(PostgresqlProvider.Instance.GetDatabaseType(typeof(int), EnumStorage.AsInteger));

        table.Columns.ShouldContain(column);
    }

    [Fact]
    public void smoke_test_writing_table_code_with_columns()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        var rules = new PostgresqlMigrator { TableCreation = CreationStyle.DropThenCreate };

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

        var rules = new PostgresqlMigrator { TableCreation = CreationStyle.CreateIfNotExists };

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

    [Fact]
    public void add_bigserial_as_default_value()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey().BigSerial();

        var sql = states.ToBasicCreateTableSql();

        sql.ShouldContain("id BIGSERIAL");
    }

    [Fact]
    public void add_smallserial_as_default_value()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey().SmallSerial();

        var sql = states.ToBasicCreateTableSql();

        sql.ShouldContain("id SMALLSERIAL");
    }

    [Fact]
    public void schema_migration_uses_create_if_not_exists_for_create_deltas()
    {
        var table = new Table("newschema.new_table");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        var delta = new SimpleSchemaDelta(table, SchemaPatchDifference.Create);
        var migration = new SchemaMigration(new[] { delta });

        var migrator = new PostgresqlMigrator();

        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);

        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE TABLE IF NOT EXISTS");
        ddl.ShouldNotContain("DROP TABLE IF EXISTS");
    }

    [Fact]
    public void adding_new_schema_does_not_drop_existing_tables_in_other_schemas()
    {
        var existingTableInPublic = new Table("public.existing_table");
        existingTableInPublic.AddColumn<int>("id").AsPrimaryKey();
        existingTableInPublic.AddColumn<string>("name");

        var existingTableInOtherSchema = new Table("otherschema.existing_table");
        existingTableInOtherSchema.AddColumn<int>("id").AsPrimaryKey();
        existingTableInOtherSchema.AddColumn<string>("data");

        var newTableInNewSchema = new Table("newschema.new_table");
        newTableInNewSchema.AddColumn<int>("id").AsPrimaryKey();
        newTableInNewSchema.AddColumn<string>("value");

        var deltas = new ISchemaObjectDelta[]
        {
            new SimpleSchemaDelta(existingTableInPublic, SchemaPatchDifference.None),
            new SimpleSchemaDelta(existingTableInOtherSchema, SchemaPatchDifference.None),
            new SimpleSchemaDelta(newTableInNewSchema, SchemaPatchDifference.Create),
        };

        var migration = new SchemaMigration(deltas);
        var migrator = new PostgresqlMigrator();

        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, migrator, AutoCreate.CreateOrUpdate);

        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE TABLE IF NOT EXISTS newschema.new_table");
        ddl.ShouldNotContain("DROP TABLE IF EXISTS newschema.new_table");

        ddl.ShouldNotContain("public.existing_table");
        ddl.ShouldNotContain("otherschema.existing_table");
        ddl.ShouldNotContain("DROP TABLE IF EXISTS public.existing_table");
        ddl.ShouldNotContain("DROP TABLE IF EXISTS otherschema.existing_table");
    }

    private class SimpleSchemaDelta : ISchemaObjectDelta
    {
        private readonly ISchemaObject _schemaObject;

        public SimpleSchemaDelta(ISchemaObject schemaObject, SchemaPatchDifference difference)
        {
            _schemaObject = schemaObject;
            Difference = difference;
        }

        public ISchemaObject SchemaObject => _schemaObject;
        public SchemaPatchDifference Difference { get; }

        public void WriteUpdate(Migrator rules, TextWriter writer)
        {
        }

        public void WriteRollback(Migrator rules, TextWriter writer)
        {
        }

        public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
        {
        }
    }
}
