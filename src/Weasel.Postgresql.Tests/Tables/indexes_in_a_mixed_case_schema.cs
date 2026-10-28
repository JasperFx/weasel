using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     The third schema name the index introspection query could not read back, and the one nobody
///     reported: a schema whose name is mixed-case.
/// </summary>
/// <remarks>
///     <para>
///         The query matched rows on <c>regclassout</c>'s rendering of the table name, and
///         <c>regclassout</c> quotes whatever has to be quoted — a reserved word (weasel#750), and
///         equally a name carrying uppercase:
///     </para>
///     <code>
///         "order".docs        -- reserved word, weasel#750
///         "MySchema".docs     -- mixed case, this test
///     </code>
///     <para>
///         None of the four candidates the query compared against quoted the schema, so both rendered
///         to something that matched nothing, and every index of the table was read back as absent.
///         From there it is weasel#504 again: <c>ItemDelta</c> calls every declared index
///         <c>Missing</c> rather than <c>Different</c>, the patch is a bare <c>create index</c>, and
///         the second run answers <c>42P07: relation already exists</c> and takes the rest of the
///         migration with it.
///     </para>
///     <para>
///         Not the same thing as weasel#224, which is a mixed-case <em>table</em> in a lower-case
///         schema (<c>mixed_case_table_with_index</c>). That one worked, because the quoted-name
///         candidates added for it covered the table's own name. The schema's was never quoted in any
///         of them.
///     </para>
///     <para>
///         Fixed by returning the table's bare <c>relname</c> instead of the rendering, so there is
///         no spelling left to anticipate. Kept as its own test because the fix arrived aimed at
///         reserved words and this case is only implied by it.
///     </para>
/// </remarks>
[Collection("mixed_case_schema")]
public class indexes_in_a_mixed_case_schema: IntegrationContext
{
    public indexes_in_a_mixed_case_schema(): base("MixedCaseSchema750")
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
    ///     And the failure that made the reserved-word half of this visible: applying the same
    ///     configuration a second time re-issued <c>create index</c> and PostgreSQL answered
    ///     <c>42P07</c>.
    /// </summary>
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
