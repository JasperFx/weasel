using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;
using Xunit;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations;

/// <summary>
///     weasel#615. Every index used to become a typed <see cref="CreateIndexOperation" />, whose
///     <c>Columns</c> is a list of identifiers that the provider quotes one by one. Two different
///     things went wrong with that, and only one of them was noisy.
///     <para>
///         A Marten computed index carries its expression as an entry in <c>Columns</c>, so
///         <c>(data -&gt;&gt; 'Kind')</c> was emitted as a column name and the migration died on
///         <c>42703: column "(data -&gt;&gt; 'Kind')" does not exist</c>. Loud, and at least
///         unmissable.
///     </para>
///     <para>
///         An operator class is the quiet one. <c>CreateIndexOperation</c> has nowhere to put
///         <c>jsonb_path_ops</c>, so the GIN index applied cleanly with the default
///         <c>jsonb_ops</c> — a different index from the one declared, on a schema that reported
///         success. That is the case worth keeping tests on.
///     </para>
/// </summary>
public class indexes_that_ef_cannot_model
{
    private static MigrationOperationTranslationOptions pgOptions() =>
        new(EfMigrationProvider.PostgreSql) { Migrator = new PostgresqlMigrator() };

    private static PgTable martenStyleTable(string name = "mt_doc_widget")
    {
        var table = new PgTable($"repro.{name}");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("data", "jsonb").NotNull();
        return table;
    }

    [Fact]
    public void a_computed_index_is_carried_as_raw_ddl_rather_than_a_quoted_column()
    {
        var table = martenStyleTable();
        table.Indexes.Add(new PgIndex("mt_doc_widget_idx_kind")
        {
            Columns = new[] { "(data ->> 'Kind')" }
        });

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        operations.OfType<CreateIndexOperation>()
            .ShouldBeEmpty("the expression would be quoted as an identifier and fail with 42703");

        var sql = operations.OfType<SqlOperation>().Single().Sql;
        sql.ShouldContain("(data ->> 'Kind')");
        sql.ShouldNotContain("\"(data ->> 'Kind')\"");
        sql.ShouldContain("mt_doc_widget_idx_kind");
    }

    [Fact]
    public void an_operator_class_survives_instead_of_silently_becoming_the_default()
    {
        var table = martenStyleTable("mt_doc_gadget");
        table.Indexes.Add(new PgIndex("mt_doc_gadget_idx_data")
        {
            Columns = new[] { "data" }, Method = IndexMethod.gin, Mask = "? jsonb_path_ops"
        });

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        var sql = operations.OfType<SqlOperation>().Single().Sql;
        sql.ShouldContain("USING gin");
        sql.ShouldContain("jsonb_path_ops");
    }

    /// <summary>
    ///     The table itself has to stay typed. Routing the whole table through raw SQL is the
    ///     workaround weasel#615 was filed to get rid of: it makes every later change to that
    ///     table something the snapshot differ refuses to diff.
    /// </summary>
    [Fact]
    public void the_table_around_an_untranslatable_index_stays_typed()
    {
        var table = martenStyleTable();
        table.Indexes.Add(new PgIndex("mt_doc_widget_idx_kind")
        {
            Columns = new[] { "(data ->> 'Kind')" }
        });

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        var createTable = operations.OfType<CreateTableOperation>().Single();
        createTable.Name.ShouldBe("mt_doc_widget");
        createTable.Columns.Select(x => x.Name).ShouldBe(new[] { "id", "data" });
    }

    [Fact]
    public void an_ordinary_index_is_still_translated_structurally()
    {
        var table = martenStyleTable();
        table.AddColumn<string>("kind");
        table.Indexes.Add(new PgIndex("idx_kind") { Columns = new[] { "kind" }, IsUnique = true });

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        operations.OfType<SqlOperation>().ShouldBeEmpty();
        var index = operations.OfType<CreateIndexOperation>().Single();
        index.Name.ShouldBe("idx_kind");
        index.Columns.ShouldBe(new[] { "kind" });
        index.IsUnique.ShouldBeTrue();
    }

    /// <summary>
    ///     A concurrent build is not a reason to go raw: it says nothing about the shape of the
    ///     index, and the resulting DDL cannot run inside a migration's transaction anyway.
    /// </summary>
    [Fact]
    public void a_concurrent_build_does_not_push_an_otherwise_typed_index_to_raw_sql()
    {
        var table = martenStyleTable();
        table.AddColumn<string>("kind");
        table.Indexes.Add(new PgIndex("idx_kind") { Columns = new[] { "kind" }, IsConcurrent = true });

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        operations.OfType<SqlOperation>().ShouldBeEmpty();
        operations.OfType<CreateIndexOperation>().Single().Name.ShouldBe("idx_kind");
    }

    public static TheoryData<string, Action<PgIndex>> OptionsTheTypedPathWouldLose => new()
    {
        { "operator class", x => x.Mask = "? jsonb_path_ops" },
        { "sort order", x => x.SortOrder = SortOrder.Desc },
        { "nulls order", x => x.NullsSortOrder = NullsSortOrder.First },
        { "collation", x => x.Collation = "C" },
        { "tablespace", x => x.TableSpace = "fast_disk" },
        { "storage parameter", x => x.FillFactor = 70 }
    };

    [Theory]
    [MemberData(nameof(OptionsTheTypedPathWouldLose))]
    public void every_option_create_index_cannot_carry_forces_the_raw_path(string _, Action<PgIndex> configure)
    {
        var table = martenStyleTable();
        table.AddColumn<string>("kind");

        var index = new PgIndex("idx_kind") { Columns = new[] { "kind" } };
        configure(index);
        table.Indexes.Add(index);

        var operations = ((ITable)table).ToMigrationOperations(pgOptions());

        operations.OfType<CreateIndexOperation>().ShouldBeEmpty();
        operations.OfType<SqlOperation>().ShouldHaveSingleItem();
    }

    /// <summary>
    ///     The diff side of the same gap. <c>SnapshotIndex</c> only captured the properties the
    ///     typed operation could carry, so <c>HasSameDefinition</c> compared a subset and reported
    ///     "unchanged" for a change to anything outside it -- no migration, no warning.
    /// </summary>
    [Fact]
    public void changing_an_operator_class_is_detected_by_the_snapshot_differ()
    {
        var options = pgOptions();

        var before = martenStyleTable("mt_doc_gadget");
        before.Indexes.Add(new PgIndex("mt_doc_gadget_idx_data")
        {
            Columns = new[] { "data" }, Method = IndexMethod.gin, Mask = "? jsonb_path_ops"
        });

        var after = martenStyleTable("mt_doc_gadget");
        after.Indexes.Add(new PgIndex("mt_doc_gadget_idx_data")
        {
            Columns = new[] { "data" }, Method = IndexMethod.gin
        });

        var diff = EfSnapshotDiffer.Diff(
            EfSchemaSnapshot.FromSchemaObjects([before], options),
            EfSchemaSnapshot.FromSchemaObjects([after], options),
            options);

        diff.HasChanges.ShouldBeTrue("dropping jsonb_path_ops is a different index, not the same one");
    }

    [Fact]
    public void an_unchanged_computed_index_does_not_diff()
    {
        var options = pgOptions();

        PgTable build()
        {
            var table = martenStyleTable();
            table.Indexes.Add(new PgIndex("mt_doc_widget_idx_kind")
            {
                Columns = new[] { "(data ->> 'Kind')" }
            });
            return table;
        }

        var diff = EfSnapshotDiffer.Diff(
            EfSchemaSnapshot.FromSchemaObjects([build()], options),
            EfSchemaSnapshot.FromSchemaObjects([build()], options),
            options);

        diff.HasChanges.ShouldBeFalse();
    }

    [Fact]
    public void a_raw_sql_index_round_trips_through_the_serialized_snapshot()
    {
        var options = pgOptions();
        var table = martenStyleTable();
        table.Indexes.Add(new PgIndex("mt_doc_widget_idx_kind")
        {
            Columns = new[] { "(data ->> 'Kind')" }
        });

        var snapshot = EfSchemaSnapshot.FromSchemaObjects([table], options);
        var rehydrated = EfSchemaSnapshot.FromJson(snapshot.ToJson());

        EfSnapshotDiffer.Diff(rehydrated, snapshot, options).HasChanges.ShouldBeFalse();
    }
}
