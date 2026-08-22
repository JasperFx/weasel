using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#504. The index introspection query carried <c>NOT nspname LIKE 'pg%'</c> alongside the
///     <c>nspname = :schema</c> it was already filtered by.
/// </summary>
/// <remarks>
///     <para>
///         The intent was to skip <c>pg_catalog</c> and <c>pg_toast</c>, but it is a bare prefix match
///         applied to the user's own schema, so any schema named <c>pgcontrol</c>, <c>pgqueues</c>,
///         <c>pgdata</c> — anything starting with those two letters — had <em>no index read back at
///         all</em>.
///     </para>
///     <para>
///         What makes it hard to spot from the outside is that the primary key still arrives: the
///         <c>isPrimary</c> branch in <c>readIndexesAsync</c> returns before the table-name check. The
///         table looks correct and only its non-PK indexes are missing.
///     </para>
///     <para>
///         With <c>actual.Indexes</c> empty, <c>ItemDelta</c> calls every declared index
///         <c>Missing</c> rather than <c>Different</c>, so the patch is a bare <c>create index</c> with
///         no preceding drop. The second startup gets <c>42P07: relation already exists</c>, and since
///         a delta runs as one command that first failure aborts everything ordered after it. The
///         schema never converges, so <c>db-assert</c> can never pass either.
///     </para>
///     <para>
///         Found in Wolverine, whose control queue storage uses a <c>pgcontrol</c> schema
///         (JasperFx/wolverine#3997).
///     </para>
/// </remarks>
[Collection("pg_prefixed")]
public class indexes_in_a_pg_prefixed_schema: IntegrationContext
{
    // The reporter's schema name. Legal: PostgreSQL reserves the "pg_" prefix, not "pg".
    public indexes_in_a_pg_prefixed_schema(): base("pg3997")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private Table BuildTable()
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "docs"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body");

        var index = new IndexDefinition("idx_docs_body");
        index.AgainstColumns("body");
        table.Indexes.Add(index);

        return table;
    }

    [Fact]
    public async Task the_index_is_read_back_from_the_database()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);

        var existing = await BuildTable().FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing!.Indexes.Select(x => x.Name).ShouldContain("idx_docs_body");
    }

    /// <summary>
    ///     The symptom as an operator meets it: the migration never settles.
    /// </summary>
    [Fact]
    public async Task the_migration_converges()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable()))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     And the failure that made it visible: applying the same configuration a second time
    ///     re-issued <c>create index</c> and PostgreSQL answered <c>42P07</c>, taking the rest of the
    ///     migration down with it.
    /// </summary>
    [Fact]
    public async Task applying_twice_does_not_throw()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);
    }
}
