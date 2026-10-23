using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     A concurrent index added to an existing partitioned table names each partition's index
///     <c>{index}_{suffix}</c>. When that runs past 63 bytes PostgreSQL truncates it silently, so two
///     partitions whose suffixes share a prefix ask for the same child name.
/// </summary>
[Collection("long_child_index_names")]
public class long_child_index_names_on_partitioned_tables: IntegrationContext
{
    // 61 characters: a valid name on its own, too long once a partition suffix is appended.
    private const string IndexName = "mt_doc_pre_go_live_care_34_idx_ngram_clientnamevolledigenaam";

    public long_child_index_names_on_partitioned_tables(): base("long_child_index_names")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private Table BuildTable(bool withIndex)
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "care_docs"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull()
            .PartitionByListValues()
            .AddPartition("01058895", "01058895")
            .AddPartition("01023456", "01023456");
        table.AddColumn<string>("body");

        if (withIndex)
        {
            var index = new IndexDefinition(IndexName) { IsConcurrent = true };
            index.AgainstColumns("body");
            table.Indexes.Add(index);
        }

        return table;
    }

    private async Task ApplyAsync(Table table) =>
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, table), AutoCreate.CreateOrUpdate);

    [Fact]
    public void every_child_index_name_fits_and_is_distinct()
    {
        var table = BuildTable(withIndex: true);
        var index = table.Indexes.Single();

        var names = table.Partitioning!.PartitionTableNames(table)
            .Select(partition => index.ChildIndexName(table, partition))
            .ToList();

        names.ShouldAllBe(name => name.Length <= PostgresqlIdentifier.DefaultMaxLength);
        names.Distinct().Count().ShouldBe(names.Count);

        // The suffix stays readable, and the name is the same on every call -- a rerun after a
        // half-finished build has to find the child it left behind.
        names.ShouldContain(name => name.EndsWith("_01058895"));
        names.ShouldContain(name => name.EndsWith("_01023456"));
        names.ShouldBe(table.Partitioning!.PartitionTableNames(table)
            .Select(partition => index.ChildIndexName(table, partition)));
    }

    [Fact]
    public void a_child_index_name_that_fits_is_unchanged()
    {
        var table = BuildTable(withIndex: false);
        var index = new IndexDefinition("idx_care_docs_body");

        index.ChildIndexName(table, "care_docs_01058895").ShouldBe("idx_care_docs_body_01058895");
    }

    [Fact]
    public async Task a_long_concurrent_index_can_be_added_to_an_existing_partitioned_table()
    {
        await ApplyAsync(BuildTable(withIndex: false));

        // Pre-fix: both partitions' indexes were "mt_doc_pre_go_live_care_34_idx_ngram_clientnamevolledigenaam_01"
        // once truncated, so the second failed -- 2BP01 dropping the first one's attached child, or
        // 42P07 building beside it before the leftover drop existed.
        var withIndex = BuildTable(withIndex: true);
        await ApplyAsync(withIndex);

        await ShouldBeFullyBuiltAsync(withIndex);
    }

    /// <summary>
    ///     What a failed run of an earlier version leaves behind: an invalid parent with the first
    ///     partition's index attached under the truncated name.
    /// </summary>
    [Fact]
    public async Task a_build_left_behind_under_the_truncated_name_is_finished_by_the_next_run()
    {
        await ApplyAsync(BuildTable(withIndex: false));

        var truncated = $"{IndexName}_01058895"[..PostgresqlIdentifier.DefaultMaxLength];
        await theConnection.CreateCommand(
                $"create index {IndexName} on only {SchemaName}.care_docs (body);"
                + $"create index {truncated} on {SchemaName}.care_docs_01058895 (body);"
                + $"alter index {SchemaName}.{IndexName} attach partition {SchemaName}.{truncated};")
            .ExecuteNonQueryAsync();

        var withIndex = BuildTable(withIndex: true);
        await ApplyAsync(withIndex);

        await ShouldBeFullyBuiltAsync(withIndex);
    }

    private async Task ShouldBeFullyBuiltAsync(Table withIndex)
    {
        var parentIsValid = await theConnection.CreateCommand(
                "select i.indisvalid from pg_index i "
                + "join pg_class c on c.oid = i.indexrelid "
                + "join pg_namespace n on n.oid = c.relnamespace "
                + $"where n.nspname = '{SchemaName}' and c.relname = '{IndexName}'")
            .ExecuteScalarAsync();
        parentIsValid.ShouldBe(true, "not every partition's index was attached");

        var attached = await theConnection.CreateCommand(
                "select count(*) from pg_inherits h "
                + "join pg_class parent on parent.oid = h.inhparent "
                + "join pg_namespace n on n.oid = parent.relnamespace "
                + $"where n.nspname = '{SchemaName}' and parent.relname = '{IndexName}'")
            .ExecuteScalarAsync();
        Convert.ToInt32(attached).ShouldBe(withIndex.Partitioning!.PartitionTableNames(withIndex).Count());

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable(withIndex: true)))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
