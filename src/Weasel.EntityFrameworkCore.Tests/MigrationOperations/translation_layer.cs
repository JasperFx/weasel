using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Shouldly;
using Weasel.Core;
using Weasel.EntityFrameworkCore;
using Weasel.Postgresql;
using Xunit;
using Weasel.Postgresql.Tables;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgSequence = Weasel.Postgresql.Sequence;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;
using SsTable = Weasel.SqlServer.Tables.Table;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     DB-free tests for the Weasel model → EF MigrationOperation translation
///     layer (#365). End-to-end validation (operations → SQL → schema
///     comparison) belongs to the inverted-harness issue.
/// </summary>
public class translation_layer
{
    private static MigrationOperationTranslationOptions pgOptions() =>
        new(EfMigrationProvider.PostgreSql) { Migrator = new PostgresqlMigrator() };

    private static MigrationOperationTranslationOptions ssOptions() =>
        new(EfMigrationProvider.SqlServer) { Migrator = new Weasel.SqlServer.SqlServerMigrator() };

    [Fact]
    public void translates_a_postgresql_table_to_a_create_table_operation()
    {
        var table = new PgTable("app.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        // set IsAutoNumber directly — the modern identity flag the EF mapping
        // uses (the fluent AutoIncrement() swaps the type to legacy SERIAL)
        table.ColumnFor("id")!.IsAutoNumber = true;
        table.AddColumn<string>("name").NotNull();
        table.AddColumn("payload", "jsonb");
        table.ColumnFor("name")!.DefaultExpression = "'unknown'";
        ((ITable)table).AddCheckConstraint("ck_orders_name", "length(name) > 0");

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        operations[0].ShouldBeOfType<EnsureSchemaOperation>().Name.ShouldBe("app");

        var create = operations.OfType<CreateTableOperation>().Single();
        create.Name.ShouldBe("orders");
        create.Schema.ShouldBe("app");

        var id = create.Columns.Single(x => x.Name == "id");
        id.ColumnType.ShouldBe("integer");
        id.ClrType.ShouldBe(typeof(int));
        id.IsNullable.ShouldBeFalse();
        id[MigrationOperationTranslation.NpgsqlValueGenerationStrategy].ShouldBe("IdentityByDefaultColumn");

        var name = create.Columns.Single(x => x.Name == "name");
        name.IsNullable.ShouldBeFalse();
        name.DefaultValueSql.ShouldBe("'unknown'");

        var payload = create.Columns.Single(x => x.Name == "payload");
        payload.ColumnType.ShouldBe("jsonb");
        payload.IsNullable.ShouldBeTrue();

        create.PrimaryKey.ShouldNotBeNull();
        create.PrimaryKey!.Columns.ShouldBe(new[] { "id" });
        create.PrimaryKey.Name.ShouldBe(table.PrimaryKeyName);

        var check = create.CheckConstraints.Single();
        check.Name.ShouldBe("ck_orders_name");
        check.Sql.ShouldBe("length(name) > 0");
    }

    [Fact]
    public void default_schema_is_omitted_and_gets_no_ensure_schema()
    {
        var pg = new PgTable("public.things");
        pg.AddColumn<int>("id").AsPrimaryKey();

        var pgOps = ((ITable)pg).ToMigrationOperations(pgOptions());
        pgOps.OfType<EnsureSchemaOperation>().ShouldBeEmpty();
        pgOps.OfType<CreateTableOperation>().Single().Schema.ShouldBeNull();

        var ss = new SsTable("dbo.things");
        ss.AddColumn<int>("id").AsPrimaryKey();

        var ssOps = ((ITable)ss).ToMigrationOperations(ssOptions());
        ssOps.OfType<EnsureSchemaOperation>().ShouldBeEmpty();
        ssOps.OfType<CreateTableOperation>().Single().Schema.ShouldBeNull();
    }

    [Fact]
    public void sql_server_identity_annotation()
    {
        var table = new SsTable("dbo.orders");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();

        var create = ((ITable)table).ToMigrationOperations(ssOptions())
            .OfType<CreateTableOperation>().Single();

        create.Columns.Single(x => x.Name == "id")[MigrationOperationTranslation.SqlServerIdentity]
            .ShouldBe("1, 1");
    }

    [Fact]
    public void computed_columns_translate_to_computed_column_sql()
    {
        var pg = new PgTable("public.people");
        pg.AddColumn<string>("first_name");
        pg.AddColumn<string>("last_name");
        pg.AddColumn("full_name", "text");
        pg.ColumnFor("full_name")!.ComputedExpression = "first_name || ' ' || last_name";

        var create = ((ITable)pg).ToMigrationOperations(pgOptions())
            .OfType<CreateTableOperation>().Single();

        var fullName = create.Columns.Single(x => x.Name == "full_name");
        fullName.ComputedColumnSql.ShouldBe("first_name || ' ' || last_name");
        fullName.IsStored.ShouldBe(true);

        var ss = new SsTable("dbo.people");
        ss.AddColumn<string>("first_name");
        ss.AddColumn<string>("full_name");
        ss.ColumnFor("full_name")!.ComputedExpression = "first_name + '!'";

        var ssCreate = ((ITable)ss).ToMigrationOperations(ssOptions())
            .OfType<CreateTableOperation>().Single();

        var ssFullName = ssCreate.Columns.Single(x => x.Name == "full_name");
        ssFullName.ComputedColumnSql.ShouldBe("first_name + '!'");
        ssFullName.IsStored.ShouldBe(false);
    }

    [Fact]
    public void indexes_translate_with_filter_includes_and_method()
    {
        var table = new PgTable("public.docs");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("kind");
        table.AddColumn("payload", "jsonb");

        var unique = new PgIndex("idx_docs_kind")
        {
            IsUnique = true,
            Columns = new[] { "kind" },
            Predicate = "kind is not null",
            IncludeColumns = new[] { "payload" }
        };
        table.Indexes.Add(unique);

        var gin = new PgIndex("idx_docs_payload")
        {
            Columns = new[] { "payload" }, Method = IndexMethod.gin
        };
        table.Indexes.Add(gin);

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());
        var indexes = operations.OfType<CreateIndexOperation>().ToArray();

        var uniqueOp = indexes.Single(x => x.Name == "idx_docs_kind");
        uniqueOp.IsUnique.ShouldBeTrue();
        uniqueOp.Columns.ShouldBe(new[] { "kind" });
        uniqueOp.Filter.ShouldBe("kind is not null");
        uniqueOp[MigrationOperationTranslation.NpgsqlIndexInclude].ShouldBe(new[] { "payload" });

        var ginOp = indexes.Single(x => x.Name == "idx_docs_payload");
        ginOp[MigrationOperationTranslation.NpgsqlIndexMethod].ShouldBe("gin");
    }

    [Fact]
    public void expression_index_without_columns_is_not_supported()
    {
        var table = new PgTable("public.docs");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.Indexes.Add(new PgIndex("idx_expression"));

        Should.Throw<NotSupportedException>(() => ((ITable)table).ToMigrationOperations(pgOptions()))
            .Message.ShouldContain("idx_expression");
    }

    [Fact]
    public void foreign_keys_translate_with_referential_actions()
    {
        var table = new PgTable("app.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("customer_id")
            .ForeignKeyTo("app.customers", "id", "fk_orders_customer",
                Weasel.Core.CascadeAction.SetNull, Weasel.Core.CascadeAction.NoAction);

        var create = ((ITable)table).ToMigrationOperations(pgOptions())
            .OfType<CreateTableOperation>().Single();

        var fk = create.ForeignKeys.Single();
        fk.Name.ShouldBe("fk_orders_customer");
        fk.Columns.ShouldBe(new[] { "customer_id" });
        fk.PrincipalTable.ShouldBe("customers");
        fk.PrincipalSchema.ShouldBe("app");
        fk.PrincipalColumns.ShouldBe(new[] { "id" });
        fk.OnDelete.ShouldBe(ReferentialAction.SetNull);
    }

    [Fact]
    public void restrict_normalizes_to_no_action_on_sql_server_only()
    {
        var pg = new PgTable("public.orders");
        pg.AddColumn<int>("id").AsPrimaryKey();
        pg.AddColumn<int>("customer_id")
            .ForeignKeyTo("public.customers", "id", "fk_pg",
                Weasel.Core.CascadeAction.Restrict, Weasel.Core.CascadeAction.NoAction);

        ((ITable)pg).ToMigrationOperations(pgOptions())
            .OfType<CreateTableOperation>().Single()
            .ForeignKeys.Single().OnDelete.ShouldBe(ReferentialAction.Restrict);

        var ss = new SsTable("dbo.orders");
        ss.AddColumn<int>("id").AsPrimaryKey();
        var fk = ((ITable)ss).AddForeignKey("fk_ss", new DbObjectName("dbo", "customers"),
            new[] { "customer_id" }, new[] { "id" });
        fk.DeleteAction = Weasel.Core.CascadeAction.Restrict;
        ss.AddColumn<int>("customer_id");

        ((ITable)ss).ToMigrationOperations(ssOptions())
            .OfType<CreateTableOperation>().Single()
            .ForeignKeys.Single().OnDelete.ShouldBe(ReferentialAction.NoAction);
    }

    [Fact]
    public void sequences_translate_to_create_sequence_operations()
    {
        var sequence = new PgSequence(new DbObjectName("app", "order_numbers"), 100)
        {
            IncrementBy = 10
        };

        var operations = new ISchemaObject[] { sequence }.ToMigrationOperations(pgOptions());

        operations.OfType<EnsureSchemaOperation>().Single().Name.ShouldBe("app");
        var create = operations.OfType<CreateSequenceOperation>().Single();
        create.Name.ShouldBe("order_numbers");
        create.Schema.ShouldBe("app");
        create.StartValue.ShouldBe(100);
        create.IncrementBy.ShouldBe(10);
    }

    [Fact]
    public void force_raw_sql_routes_a_table_through_its_own_ddl()
    {
        var table = new PgTable("app.partitioned");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey();
        table.PartitionByList("tenant_id");

        var options = pgOptions();
        options.ForceRawSql = o => o is PgTable { Partitioning: not null };

        var operations = ((ITable)table).ToMigrationOperations(options);

        var sql = operations.OfType<SqlOperation>().Single();
        sql.Sql.ShouldContain("CREATE TABLE", Case.Insensitive);
        sql.Sql.ShouldContain("app.partitioned", Case.Insensitive);
        sql.Sql.ShouldContain("PARTITION BY LIST", Case.Insensitive);
        operations.OfType<CreateTableOperation>().ShouldBeEmpty();
    }

    [Fact]
    public void raw_sql_fallback_without_a_migrator_throws()
    {
        var table = new PgTable("app.partitioned");
        table.AddColumn<int>("id").AsPrimaryKey();

        var options = new MigrationOperationTranslationOptions(EfMigrationProvider.PostgreSql)
        {
            ForceRawSql = _ => true
        };

        Should.Throw<InvalidOperationException>(() => ((ITable)table).ToMigrationOperations(options))
            .Message.ShouldContain("Migrator");
    }

    [Fact]
    public void drop_operations_reverse_the_object_order()
    {
        var sequence = new PgSequence(new DbObjectName("app", "order_numbers"));
        var customers = new PgTable("app.customers");
        customers.AddColumn<int>("id").AsPrimaryKey();
        var orders = new PgTable("app.orders");
        orders.AddColumn<int>("id").AsPrimaryKey();

        var operations = new ISchemaObject[] { sequence, customers, orders }
            .ToDropMigrationOperations(pgOptions());

        operations.Count.ShouldBe(3);
        operations[0].ShouldBeOfType<DropTableOperation>().Name.ShouldBe("orders");
        operations[1].ShouldBeOfType<DropTableOperation>().Name.ShouldBe("customers");
        var dropSequence = operations[2].ShouldBeOfType<DropSequenceOperation>();
        dropSequence.Name.ShouldBe("order_numbers");
        dropSequence.Schema.ShouldBe("app");
    }

    [Fact]
    public void preserve_identifier_case_flows_through_to_operation_names()
    {
        var table = new PgTable("public.Blogs");
        table.PreserveIdentifierCase = true;
        table.AddColumn<int>("BlogId").AsPrimaryKey();
        table.AddColumn<string>("Url").NotNull();

        var create = ((ITable)table).ToMigrationOperations(pgOptions())
            .OfType<CreateTableOperation>().Single();

        create.Name.ShouldBe("Blogs");
        create.Columns.Select(x => x.Name).ShouldBe(new[] { "BlogId", "Url" });
    }
}
