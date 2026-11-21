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
            return table;
        }).ToArray();

        // The tables have to exist: a missing table is answered by the wrapper alone, and never runs (or
        // compiles) the catalog reads this is about.
        foreach (var table in tables)
        {
            await CreateSchemaObjectInDatabase(table);
        }

        var before = await catalogReadPlanUseCounts();
        await SchemaMigration.DetermineAsync(theConnection, tables.Cast<ISchemaObject>().ToArray());
        var after = await catalogReadPlanUseCounts();

        // One cached plan served every table. Twelve plans used once each would sum to twelve too, so this
        // looks for a single plan whose use count went up by at least that much.
        var mostUsesOfOnePlan = after
            .Select(x => x.Value - before.GetValueOrDefault(x.Key))
            .DefaultIfEmpty(0)
            .Max();

        mostUsesOfOnePlan.ShouldBeGreaterThanOrEqualTo(tables.Length);
    }

    /// <summary>
    ///     Use counts of the cached plans for the catalog reads in this database, by plan handle. The small
    ///     wrapper that resolves the table's name to its id is not one of them: its text carries theirs as a
    ///     literal, but it is a zero-cost batch that SQL Server compiles on every call and never caches (on
    ///     2019, 2022 and 2025 alike), so matching it reads nothing at all.
    /// </summary>
    private async Task<Dictionary<string, long>> catalogReadPlanUseCounts()
    {
        await using var cmd = theConnection.CreateCommand("""
            select convert(varchar(130), cp.plan_handle, 1), cast(cp.usecounts as bigint)
            from sys.dm_exec_cached_plans cp
                cross apply sys.dm_exec_sql_text(cp.plan_handle) st
            where st.dbid = db_id()
                and st.text like N'(@id int)%from sys.computed_columns comp%'
            """);

        var counts = new Dictionary<string, long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) counts[reader.GetString(0)] = reader.GetInt64(1);
        return counts;
    }
}
