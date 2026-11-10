using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class batched_table_introspection: IntegrationContext
{
    public batched_table_introspection(): base("batched_intro")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private static Table parent()
    {
        var table = new Table("batched_intro.parent");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("code", "varchar(12)").NotNull().AddIndex(x => x.IsUnique = true);
        table.AddColumn("amount", "decimal(18,2)").DefaultValue(0);
        table.CheckConstraints.Add(new TableCheckConstraint("ck_parent_amount", "amount >= 0"));
        return table;
    }

    private static Table child()
    {
        var table = new Table("batched_intro.child");
        table.AddColumn<int>("tenant").AsPrimaryKey();
        table.AddColumn<long>("seq").AsPrimaryKey();
        table.AddColumn<int>("parent_id").ForeignKeyTo("batched_intro.parent", "id");
        table.AddColumn("stamp", "datetime2(3)").AllowNulls();
        table.AddColumn<string>("label").AllowNulls();
        table.AddColumn<string>("shout").ComputedAs("upper(label)");
        return table;
    }

    private static Table quoted()
    {
        var table = new Table(new SqlServerObjectName("batched_intro", "o'brien]s"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("note", "nvarchar(max)").AllowNulls();
        return table;
    }

    private static Table missing()
    {
        var table = new Table("batched_intro.missing");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    private static Table[] everything() => [parent(), child(), quoted(), missing()];

    private async Task createAllButMissing()
    {
        foreach (var table in everything().Where(x => x.Identifier.Name != "missing"))
        {
            await CreateSchemaObjectInDatabase(table);
        }
    }

    [Fact]
    public async Task each_table_in_one_batch_reads_back_only_its_own_structure()
    {
        await createAllButMissing();

        var migration = await SchemaMigration.DetermineAsync(theConnection, everything().Cast<ISchemaObject>().ToArray());

        var deltas = migration.Deltas.Cast<TableDelta>().ToArray();
        deltas.Select(x => x.Difference).ShouldBe([
            SchemaPatchDifference.None, SchemaPatchDifference.None, SchemaPatchDifference.None,
            SchemaPatchDifference.Create
        ]);

        var parentActual = deltas[0].Actual!;
        parentActual.Columns.Select(x => x.Name).ShouldBe(["id", "code", "amount"]);
        parentActual.ColumnFor("amount")!.Type.ShouldBe("decimal(18,2)");
        parentActual.Indexes.Count.ShouldBe(1);
        parentActual.CheckConstraints.Single().Name.ShouldBe("ck_parent_amount");
        parentActual.ForeignKeys.ShouldBeEmpty();

        var childActual = deltas[1].Actual!;
        childActual.PrimaryKeyColumns.ShouldBe(["tenant", "seq"]);
        childActual.ColumnFor("stamp")!.Type.ShouldBe("datetime2(3)");
        childActual.ColumnFor("shout")!.ComputedExpression.ShouldNotBeNull();
        childActual.ForeignKeys.Single().LinkedTable.ShouldBe(new SqlServerObjectName("batched_intro", "parent"));
        childActual.Indexes.ShouldBeEmpty();
        childActual.CheckConstraints.ShouldBeEmpty();

        var quotedActual = deltas[2].Actual!;
        quotedActual.Columns.Select(x => x.Name).ShouldBe(["id", "note"]);
        quotedActual.ColumnFor("note")!.Type.ShouldBe("nvarchar(max)");

        deltas[3].Actual.ShouldBeNull();
    }

    [Fact]
    public async Task batched_read_back_matches_reading_each_table_on_its_own()
    {
        await createAllButMissing();

        var migration = await SchemaMigration.DetermineAsync(theConnection, everything().Cast<ISchemaObject>().ToArray());

        foreach (var delta in migration.Deltas.Cast<TableDelta>())
        {
            var alone = await delta.Expected.FetchExistingAsync(theConnection);
            if (alone is null)
            {
                delta.Actual.ShouldBeNull();
                continue;
            }

            var batched = delta.Actual!;
            createSql(batched).ShouldBe(createSql(alone));
            batched.Columns.Select(x => x.ToDeclaration()).ShouldBe(alone.Columns.Select(x => x.ToDeclaration()));
            batched.PrimaryKeyName.ShouldBe(alone.PrimaryKeyName);
            batched.ForeignKeys.Select(x => x.ToDDL(batched)).ShouldBe(alone.ForeignKeys.Select(x => x.ToDDL(alone)));
            batched.Indexes.Select(x => x.ToDDL(batched)).ShouldBe(alone.Indexes.Select(x => x.ToDDL(alone)));
        }
    }

    private static string createSql(Table table)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    [Fact]
    public async Task drift_found_in_a_batch_is_reported_against_the_right_table()
    {
        await createAllButMissing();

        var changedChild = child();
        changedChild.AddColumn<int>("added").AllowNulls();

        var migration = await SchemaMigration.DetermineAsync(theConnection, parent(), changedChild, quoted());

        migration.Deltas.Select(x => x.Difference).ShouldBe([
            SchemaPatchDifference.None, SchemaPatchDifference.Update, SchemaPatchDifference.None
        ]);
        var writer = new StringWriter();
        migration.Deltas[1].WriteUpdate(new SqlServerMigrator(), writer);
        writer.ToString().ShouldContain("alter table batched_intro.child add added int NULL");
    }

    [Fact]
    public async Task introspecting_many_tables_reuses_one_compiled_plan()
    {
        var tables = Enumerable.Range(0, 12).Select(i =>
        {
            var table = new Table($"batched_intro.many{i}");
            table.AddColumn<int>("id").AsPrimaryKey();
            return (ISchemaObject)table;
        }).ToArray();

        var before = await introspectionPlanUseCount();
        await SchemaMigration.DetermineAsync(theConnection, tables);
        var after = await introspectionPlanUseCount();

        (after - before).ShouldBeGreaterThanOrEqualTo(tables.Length);
    }

    private async Task<long> introspectionPlanUseCount()
    {
        var count = await theConnection.CreateCommand("""
            select isnull(sum(cast(cp.usecounts as bigint)), 0)
            from sys.dm_exec_cached_plans cp
                cross apply sys.dm_exec_sql_text(cp.plan_handle) st
            where st.text like '(@schema sysname, @name sysname)%from sys.computed_columns comp%'
            """).ExecuteScalarAsync();
        return (long)count!;
    }
}
