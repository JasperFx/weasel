using System.Text.RegularExpressions;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#883: one migration changes a table's primary key and, on another table, the foreign key that
///     references it. Dropping the old key with CASCADE takes the foreign keys that reference it down, and
///     the referenced table's delta put back every key the other table declares, including the one that
///     table's own delta adds or rewrites itself. So the key went on twice (42710), or before the column it
///     uses existed (42703). A partitioning rebuild drops the table with CASCADE too, and put nothing back
///     when its primary key stayed the same. Each case runs with the tables in either order.
/// </summary>
[Collection("pkfkchange")]
public class a_primary_key_change_with_a_foreign_key_change: IntegrationContext
{
    private const string Schema = "pkfkchange";

    public a_primary_key_change_with_a_foreign_key_change(): base(Schema)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
    }

    private static readonly DbObjectName StreamsName = new PostgresqlObjectName(Schema, "streams");
    private static readonly DbObjectName EventsName = new PostgresqlObjectName(Schema, "events");

    /// <summary>
    ///     Keyed (id, is_archived), or (tenant_id, id, is_archived) once it is conjoined, as Marten's
    ///     mt_streams is.
    /// </summary>
    private static Table streams(bool conjoined)
    {
        var table = new Table(StreamsName);
        var tenantId = table.AddColumn<string>("tenant_id").NotNull();
        if (conjoined) tenantId.AsPrimaryKey();

        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<bool>("is_archived").AsPrimaryKey().DefaultValueByExpression("false");
        return table;
    }

    private static Table events(ForeignKey? foreignKey)
    {
        var table = new Table(EventsName);
        table.AddColumn<long>("seq_id").AsPrimaryKey();
        table.AddColumn<Guid>("stream_id").NotNull();
        table.AddColumn<string>("tenant_id").NotNull();
        table.AddColumn<bool>("is_archived").NotNull().DefaultValueByExpression("false");

        if (foreignKey != null) table.ForeignKeys.Add(foreignKey);
        return table;
    }

    private static ForeignKey toStream(string name) => new(name)
    {
        LinkedTable = StreamsName, ColumnNames = ["stream_id", "is_archived"], LinkedNames = ["id", "is_archived"]
    };

    private static ForeignKey toConjoinedStream(string name) => new(name)
    {
        LinkedTable = StreamsName,
        ColumnNames = ["tenant_id", "stream_id", "is_archived"],
        LinkedNames = ["tenant_id", "id", "is_archived"]
    };

    private static Table[] inOrder(bool referencedFirst, Table referenced, Table referencing)
        => referencedFirst ? [referenced, referencing] : [referencing, referenced];

    private async Task applyAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, tables);
        await new PostgresqlMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);
    }

    private async Task<string> renderAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, tables);
        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, new PostgresqlMigrator(), AutoCreate.CreateOrUpdate);
        return writer.ToString();
    }

    private async Task seedAsync()
    {
        await applyAsync(streams(false), events(toStream("fkey_events_stream_id_is_archived")));
    }

    /// <summary>
    ///     Every foreign key on the table, as PostgreSQL describes it. A key that references a partitioned
    ///     table has a child constraint per partition, which PostgreSQL manages, so only the parents count.
    /// </summary>
    private async Task<string[]> foreignKeysOnAsync(DbObjectName table)
    {
        await using var reader = await theConnection.CreateCommand(
                "select conname || ' ' || pg_get_constraintdef(oid) from pg_constraint " +
                "where contype = 'f' and conparentid = 0 and conrelid = :table::regclass order by conname")
            .With("table", table.QualifiedName)
            .ExecuteReaderAsync();

        var keys = new List<string>();
        while (await reader.ReadAsync()) keys.Add(reader.GetString(0));
        return keys.ToArray();
    }

    private static int timesAdded(string ddl, string foreignKeyName)
        => Regex.Matches(ddl, $@"ADD CONSTRAINT {foreignKeyName} FOREIGN KEY", RegexOptions.IgnoreCase).Count;

    private async Task theMigrationIsCompleteAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, CancellationToken.None, tables);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_foreign_key_is_replaced_by_one_with_another_name(bool referencedFirst)
    {
        await seedAsync();

        var expected = inOrder(referencedFirst, streams(true),
            events(toConjoinedStream("fkey_events_stream_id_tenant_id_is_archived")));

        timesAdded(await renderAsync(expected), "fkey_events_stream_id_tenant_id_is_archived").ShouldBe(1);

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBe([
            "fkey_events_stream_id_tenant_id_is_archived FOREIGN KEY (tenant_id, stream_id, is_archived) " +
            "REFERENCES pkfkchange.streams(tenant_id, id, is_archived)"
        ]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_foreign_key_keeps_its_name_and_changes_its_columns(bool referencedFirst)
    {
        await applyAsync(streams(false), events(toStream("fkey_events_stream")));

        var expected = inOrder(referencedFirst, streams(true), events(toConjoinedStream("fkey_events_stream")));

        timesAdded(await renderAsync(expected), "fkey_events_stream").ShouldBe(1);

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBe([
            "fkey_events_stream FOREIGN KEY (tenant_id, stream_id, is_archived) " +
            "REFERENCES pkfkchange.streams(tenant_id, id, is_archived)"
        ]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_foreign_key_is_removed(bool referencedFirst)
    {
        await seedAsync();

        var expected = inOrder(referencedFirst, streams(true), events(null));

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBeEmpty();
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_foreign_key_that_does_not_change_is_put_back(bool referencedFirst)
    {
        // The new key still covers the referenced columns through a unique index of their own
        var conjoined = streams(true);
        conjoined.Indexes.Add(new IndexDefinition("ix_streams_id_is_archived")
        {
            IsUnique = true, Columns = ["id", "is_archived"]
        });

        await seedAsync();

        var expected = inOrder(referencedFirst, conjoined, events(toStream("fkey_events_stream_id_is_archived")));

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBe([
            "fkey_events_stream_id_is_archived FOREIGN KEY (stream_id, is_archived) " +
            "REFERENCES pkfkchange.streams(id, is_archived)"
        ]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_new_foreign_key_uses_a_column_its_table_adds(bool referencedFirst)
    {
        // The shape of Marten's tag tables (marten#6081): the events table's key gains is_archived, and the
        // tag table gains the column and moves its key onto both
        var eventsV1 = new Table(EventsName);
        eventsV1.AddColumn<long>("seq_id").AsPrimaryKey();
        eventsV1.AddColumn<bool>("is_archived").NotNull().DefaultValueByExpression("false");

        var eventsV2 = new Table(EventsName);
        eventsV2.AddColumn<long>("seq_id").AsPrimaryKey();
        eventsV2.AddColumn<bool>("is_archived").AsPrimaryKey().DefaultValueByExpression("false");

        var tagsName = new PostgresqlObjectName(Schema, "tags");
        var tagsV1 = new Table(tagsName);
        tagsV1.AddColumn<string>("value").AsPrimaryKey();
        tagsV1.AddColumn<long>("seq_id").AsPrimaryKey();
        tagsV1.ForeignKeys.Add(new ForeignKey("fkey_tags_seq_id")
        {
            LinkedTable = EventsName, ColumnNames = ["seq_id"], LinkedNames = ["seq_id"]
        });

        var tagsV2 = new Table(tagsName);
        tagsV2.AddColumn<string>("value").AsPrimaryKey();
        tagsV2.AddColumn<long>("seq_id").AsPrimaryKey();
        tagsV2.AddColumn<bool>("is_archived").NotNull().DefaultValueByExpression("false");
        tagsV2.ForeignKeys.Add(new ForeignKey("fkey_tags_seq_id_is_archived")
        {
            LinkedTable = EventsName, ColumnNames = ["seq_id", "is_archived"], LinkedNames = ["seq_id", "is_archived"]
        });

        await applyAsync(eventsV1, tagsV1);

        var expected = inOrder(referencedFirst, eventsV2, tagsV2);

        timesAdded(await renderAsync(expected), "fkey_tags_seq_id_is_archived").ShouldBe(1);

        await applyAsync(expected);

        (await foreignKeysOnAsync(tagsName)).ShouldBe([
            "fkey_tags_seq_id_is_archived FOREIGN KEY (seq_id, is_archived) " +
            "REFERENCES pkfkchange.events(seq_id, is_archived)"
        ]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_partitioning_rebuild_that_keeps_the_primary_key_puts_back_the_keys_that_reference_it(
        bool referencedFirst)
    {
        // The rebuild drops the table with CASCADE, which takes the other table's key with it
        Table eventsTable(bool partitioned)
        {
            var table = new Table(EventsName);
            table.AddColumn<long>("seq_id").AsPrimaryKey();
            var isArchived = table.AddColumn<bool>("is_archived").AsPrimaryKey().DefaultValueByExpression("false");
            if (partitioned) isArchived.PartitionByListValues().AddPartition("archived", true);
            return table;
        }

        var tags = new Table(new PostgresqlObjectName(Schema, "tags"));
        tags.AddColumn<string>("value").AsPrimaryKey();
        tags.AddColumn<long>("seq_id").AsPrimaryKey();
        tags.AddColumn<bool>("is_archived").NotNull().DefaultValueByExpression("false");
        tags.ForeignKeys.Add(new ForeignKey("fkey_tags_seq_id_is_archived")
        {
            LinkedTable = EventsName, ColumnNames = ["seq_id", "is_archived"], LinkedNames = ["seq_id", "is_archived"]
        });

        await applyAsync(eventsTable(false), tags);

        var expected = inOrder(referencedFirst, eventsTable(true), tags);

        await applyAsync(expected);

        (await foreignKeysOnAsync(tags.Identifier)).ShouldBe([
            "fkey_tags_seq_id_is_archived FOREIGN KEY (seq_id, is_archived) " +
            "REFERENCES pkfkchange.events(seq_id, is_archived)"
        ]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_rendered_script_runs(bool referencedFirst)
    {
        await seedAsync();

        var expected = inOrder(referencedFirst, streams(true),
            events(toConjoinedStream("fkey_events_stream_id_tenant_id_is_archived")));

        var script = await renderAsync(expected);
        await theConnection.CreateCommand(script).ExecuteNonQueryAsync();

        (await foreignKeysOnAsync(EventsName)).Length.ShouldBe(1);
        await theMigrationIsCompleteAsync(expected);
    }
}
