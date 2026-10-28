using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     The index introspection query matched rows on <c>regclassout</c>'s rendering of the table name,
///     which quotes a reserved-word schema (<c>"order".docs</c>) that none of the candidates quoted.
/// </summary>
/// <remarks>
///     <para>
///         So no index in a schema named <c>order</c> or <c>user</c> was read back, and — as with
///         weasel#504 — every declared index was <c>Missing</c>, the patch re-issued a bare
///         <c>create index</c>, and the second startup failed with <c>42P07</c>.
///     </para>
///     <para>
///         Found in Wolverine, whose envelope storage shares a service's own schema: an <c>order</c> or
///         <c>user</c> service could start once and never again (JasperFx/wolverine#5215).
///     </para>
/// </remarks>
[Collection("reserved_word_schema")]
public class indexes_in_a_reserved_word_schema: IntegrationContext
{
    public indexes_in_a_reserved_word_schema(): base("order")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private Table BuildTable(string? primaryKeyName = null)
    {
        var table = new Table(new PostgresqlObjectName(SchemaName, "docs"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body");

        if (primaryKeyName != null)
        {
            table.PrimaryKeyName = primaryKeyName;
        }

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

    [Fact]
    public async Task the_migration_converges()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable()))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task applying_twice_does_not_throw()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable()), AutoCreate.CreateOrUpdate);
    }

    /// <summary>
    ///     The primary key's name rides the same rows as the indexes, so it was not read back either.
    ///     That is invisible for a table using the provider's default name, because
    ///     <c>PrimaryKeyName</c> falls back to exactly that default whenever nothing assigned it, and
    ///     the two then agree by construction.
    /// </summary>
    [Fact]
    public async Task a_named_primary_key_constraint_is_read_back()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable("docs_pk")), AutoCreate.CreateOrUpdate);

        var existing = await BuildTable("docs_pk").FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing!.PrimaryKeyName.ShouldBe("docs_pk");
    }

    /// <summary>
    ///     Name the constraint something of your own and the masking stops. The delta compares the
    ///     declared name against the derived default, calls it an <c>Update</c>, and emits
    ///     <c>rename constraint pkey_docs_id to docs_pk</c> — off a constraint name that was never in
    ///     the database. PostgreSQL answers <c>42704</c>, every run, for ever.
    /// </summary>
    [Fact]
    public async Task a_named_primary_key_constraint_converges()
    {
        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable("docs_pk")), AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, BuildTable("docs_pk")))
            .Difference.ShouldBe(SchemaPatchDifference.None);

        await new PostgresqlMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable("docs_pk")), AutoCreate.CreateOrUpdate);
    }
}
