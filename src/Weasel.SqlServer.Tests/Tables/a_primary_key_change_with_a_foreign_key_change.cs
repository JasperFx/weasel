using System.Text.RegularExpressions;
using JasperFx;
using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     weasel#886: one migration changes a table's primary key and, on another table, the foreign key that
///     references it. The referenced table's delta takes the keys that reference it down before it drops its
///     primary key and put every one of them back afterwards, including one the other table's own delta
///     drops or rewrites. So a key that should be gone came back against a primary key it no longer matched
///     (Msg 1776). Each case runs with the tables in either order.
/// </summary>
public class a_primary_key_change_with_a_foreign_key_change: IntegrationContext
{
    private const string Schema = "pkfkchange";

    public a_primary_key_change_with_a_foreign_key_change(): base(Schema)
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private static readonly DbObjectName StreamsName = new SqlServerObjectName(Schema, "streams");
    private static readonly DbObjectName EventsName = new SqlServerObjectName(Schema, "events");

    /// <summary>
    ///     Keyed (id), or (tenant_id, id) once it is conjoined, as Polecat's pc_streams is.
    /// </summary>
    private static Table streams(bool conjoined)
    {
        var table = new Table(StreamsName);
        var tenantId = table.AddColumn("tenant_id", "varchar(100)").NotNull();
        if (conjoined) tenantId.AsPrimaryKey();

        table.AddColumn<Guid>("id").AsPrimaryKey();
        return table;
    }

    private static Table events(ForeignKey? foreignKey)
    {
        var table = new Table(EventsName);
        table.AddColumn<long>("seq_id").AsPrimaryKey();
        table.AddColumn<Guid>("stream_id").NotNull();
        table.AddColumn("tenant_id", "varchar(100)").NotNull();

        if (foreignKey != null) table.ForeignKeys.Add(foreignKey);
        return table;
    }

    private static ForeignKey toStream(string name) => new(name)
    {
        LinkedTable = StreamsName, ColumnNames = ["stream_id"], LinkedNames = ["id"], OnDelete = CascadeAction.Cascade
    };

    private static ForeignKey toConjoinedStream(string name) => new(name)
    {
        LinkedTable = StreamsName, ColumnNames = ["tenant_id", "stream_id"], LinkedNames = ["tenant_id", "id"]
    };

    private static Table[] inOrder(bool referencedFirst, Table referenced, Table referencing)
        => referencedFirst ? [referenced, referencing] : [referencing, referenced];

    private async Task applyAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, tables);
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);
    }

    private async Task<string> renderAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, tables);
        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, new SqlServerMigrator(), AutoCreate.CreateOrUpdate);
        return writer.ToString();
    }

    private async Task seedAsync()
    {
        await applyAsync(streams(false), events(toStream("fk_events_stream_id")));
    }

    /// <summary>
    ///     Every foreign key on the table: its name, its columns and the columns they reference.
    /// </summary>
    private async Task<string[]> foreignKeysOnAsync(DbObjectName table)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = """
            select fk.name + ' (' +
                string_agg(pc.name, ', ') within group (order by fkc.constraint_column_id) + ') -> ' +
                object_name(fk.referenced_object_id) + '(' +
                string_agg(rc.name, ', ') within group (order by fkc.constraint_column_id) + ')'
            from sys.foreign_keys fk
            join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
            join sys.columns pc on pc.object_id = fkc.parent_object_id and pc.column_id = fkc.parent_column_id
            join sys.columns rc on rc.object_id = fkc.referenced_object_id and rc.column_id = fkc.referenced_column_id
            where fk.parent_object_id = object_id(@table)
            group by fk.name, fk.referenced_object_id
            order by fk.name
            """;
        cmd.Parameters.AddWithValue("@table", table.QualifiedName);

        await using var reader = await cmd.ExecuteReaderAsync();
        var keys = new List<string>();
        while (await reader.ReadAsync()) keys.Add(reader.GetString(0));
        return keys.ToArray();
    }

    private static int timesAdded(string ddl, string foreignKeyName)
        => Regex.Matches(ddl, $@"ADD CONSTRAINT {foreignKeyName} FOREIGN KEY", RegexOptions.IgnoreCase).Count;

    private async Task theMigrationIsCompleteAsync(params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, tables);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_foreign_key_is_removed(bool referencedFirst)
    {
        await seedAsync();

        var expected = inOrder(referencedFirst, streams(true), events(null));

        timesAdded(await renderAsync(expected), "fk_events_stream_id").ShouldBe(0);

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBeEmpty();
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_foreign_key_is_replaced_by_one_with_another_name(bool referencedFirst)
    {
        await seedAsync();

        var expected = inOrder(referencedFirst, streams(true), events(toConjoinedStream("fk_events_stream_id_tenant_id")));

        var ddl = await renderAsync(expected);
        timesAdded(ddl, "fk_events_stream_id_tenant_id").ShouldBe(1);
        timesAdded(ddl, "fk_events_stream_id").ShouldBe(0);

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBe(["fk_events_stream_id_tenant_id (tenant_id, stream_id) -> streams(tenant_id, id)"]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_foreign_key_keeps_its_name_and_changes_its_columns(bool referencedFirst)
    {
        await applyAsync(streams(false), events(toStream("fk_events_stream")));

        var expected = inOrder(referencedFirst, streams(true), events(toConjoinedStream("fk_events_stream")));

        timesAdded(await renderAsync(expected), "fk_events_stream").ShouldBe(1);

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBe(["fk_events_stream (tenant_id, stream_id) -> streams(tenant_id, id)"]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_foreign_key_that_does_not_change_is_put_back(bool referencedFirst)
    {
        // The old key still covers the referenced column through a unique index of its own
        var conjoined = streams(true);
        conjoined.Indexes.Add(new IndexDefinition("ix_streams_id") { IsUnique = true, Columns = ["id"] });

        await seedAsync();

        var expected = inOrder(referencedFirst, conjoined, events(toStream("fk_events_stream_id")));

        await applyAsync(expected);

        (await foreignKeysOnAsync(EventsName)).ShouldBe(["fk_events_stream_id (stream_id) -> streams(id)"]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_new_foreign_key_uses_a_column_its_table_adds(bool referencedFirst)
    {
        var tagsName = new SqlServerObjectName(Schema, "tags");

        Table tags(bool conjoined)
        {
            var table = new Table(tagsName);
            table.AddColumn("value", "varchar(100)").AsPrimaryKey();
            table.AddColumn<Guid>("stream_id").AsPrimaryKey();
            if (conjoined) table.AddColumn("tenant_id", "varchar(100)").NotNull().DefaultValueByString("*DEFAULT*");

            table.ForeignKeys.Add(conjoined
                ? new ForeignKey("fk_tags_stream_id_tenant_id")
                {
                    LinkedTable = StreamsName, ColumnNames = ["tenant_id", "stream_id"], LinkedNames = ["tenant_id", "id"]
                }
                : new ForeignKey("fk_tags_stream_id")
                {
                    LinkedTable = StreamsName, ColumnNames = ["stream_id"], LinkedNames = ["id"]
                });
            return table;
        }

        await applyAsync(streams(false), tags(false));

        var expected = inOrder(referencedFirst, streams(true), tags(true));

        timesAdded(await renderAsync(expected), "fk_tags_stream_id_tenant_id").ShouldBe(1);

        await applyAsync(expected);

        (await foreignKeysOnAsync(tagsName)).ShouldBe(["fk_tags_stream_id_tenant_id (tenant_id, stream_id) -> streams(tenant_id, id)"]);
        await theMigrationIsCompleteAsync(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_rendered_script_runs(bool referencedFirst)
    {
        await seedAsync();

        var expected = inOrder(referencedFirst, streams(true), events(null));
        var script = await renderAsync(expected);

        foreach (var batch in new SqlServerMigrator().SplitIntoBatches(script))
        {
            if (batch.Trim().IsEmpty()) continue;

            await using var cmd = theConnection.CreateCommand();
            cmd.CommandText = batch;
            await cmd.ExecuteNonQueryAsync();
        }

        (await foreignKeysOnAsync(EventsName)).ShouldBeEmpty();
        await theMigrationIsCompleteAsync(expected);
    }
}
