using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
///     weasel#677. The generated creation script has to run. Nothing short of executing it proves
///     that: every string-level assertion — the table names are all present, the model is correct —
///     passes over a script whose first constraint fails, which is how this survived in Polecat
///     (JasperFx/polecat#684) behind green <c>db-dump</c> tests.
/// </summary>
/// <remarks>
///     The ordering logic itself lives in <c>Weasel.Core</c> and is unit-tested in
///     <c>schema_object_dependency_order</c>. These run it against a real server, because the failure
///     being fixed is a server error — <c>42P01 relation "…" does not exist</c> — and because the
///     migration path's deferral has trained the caller to expect yield order not to matter.
/// </remarks>
[Collection("scriptorder")]
public class generated_script_runs_in_dependency_order: IntegrationContext
{
    public generated_script_runs_in_dependency_order(): base("scriptorder")
    {
    }

    private static Table table(string name, params string[] references)
    {
        var table = new Table(new PostgresqlObjectName("scriptorder", name));
        table.AddColumn<int>("id").AsPrimaryKey();

        foreach (var reference in references)
        {
            table.AddColumn<int>($"{reference}_id").AllowNulls();
            table.ForeignKeys.Add(new ForeignKey($"fk_{name}_to_{reference}")
            {
                LinkedTable = new PostgresqlObjectName("scriptorder", reference),
                ColumnNames = [$"{reference}_id"],
                LinkedNames = ["id"]
            });
        }

        return table;
    }

    private async Task executeAsync(string script)
    {
        await theConnection.CreateCommand(script).ExecuteNonQueryAsync();
    }

    private async Task<int> constraintCountAsync()
    {
        var count = await theConnection.CreateCommand(
                """
                select count(*) from pg_constraint c
                join pg_namespace n on n.oid = c.connamespace
                where n.nspname = 'scriptorder' and c.contype = 'f'
                """)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count);
    }

    /// <summary>
    ///     The reported failure, in its simplest form: one feature yielding a child table before its
    ///     parent. Before the fix this threw <c>42P01</c> on the first <c>REFERENCES</c>.
    /// </summary>
    [Fact]
    public async Task a_child_yielded_before_its_parent_still_runs()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("scriptorder", theDataSource);
        db.AddTable(table("so_child", "so_parent"));
        db.AddTable(table("so_parent"));

        await executeAsync(db.ToDatabaseScript());

        (await constraintCountAsync()).ShouldBe(1);
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task a_chain_yielded_backwards_still_runs()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("scriptorder", theDataSource);
        db.AddTable(table("so_c", "so_b"));
        db.AddTable(table("so_b", "so_a"));
        db.AddTable(table("so_a"));

        await executeAsync(db.ToDatabaseScript());

        (await constraintCountAsync()).ShouldBe(2);
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The case a per-feature sort cannot reach, and the reason the features themselves are
    ///     ordered too: the referenced table belongs to a different feature, and
    ///     <c>WriteScriptsByTypeAsync</c> would put it in a different file.
    /// </summary>
    [Fact]
    public async Task a_reference_across_features_still_runs()
    {
        await ResetSchema();

        var children = new ScriptOrderFeature("children");
        children.Tables.Add(table("so_x_child", "so_x_parent"));

        var parents = new ScriptOrderFeature("parents");
        parents.Tables.Add(table("so_x_parent"));

        // Deliberately the wrong way round, and in a fixed order: a LightweightCache does not
        // preserve insertion order, so a cache-backed database passed this before the fix by luck
        var db = new MultiFeatureDatabase(theDataSource, children, parents);

        await executeAsync(db.ToDatabaseScript());

        (await constraintCountAsync()).ShouldBe(1);
    }

    /// <summary>
    ///     A mutual reference has no valid creation order, so the script cannot be made to run and
    ///     the sort deliberately does not pretend otherwise — see <see cref="SchemaObjectOrdering" />.
    ///     Pinned so that the limit is a decision on record rather than a surprise, and so that the
    ///     migration path's ability to handle the same model stays visible next to it.
    /// </summary>
    [Fact]
    public async Task a_mutual_reference_is_still_beyond_a_script_but_not_beyond_a_migration()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("scriptorder", theDataSource);
        db.AddTable(table("so_cycle_a", "so_cycle_b"));
        db.AddTable(table("so_cycle_b", "so_cycle_a"));

        await Should.ThrowAsync<Npgsql.PostgresException>(() => executeAsync(db.ToDatabaseScript()));

        await ResetSchema();

        // The same model, through the path that defers one of the keys
        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        (await constraintCountAsync()).ShouldBe(2);
    }
}

internal class ScriptOrderFeature: FeatureSchemaBase
{
    public ScriptOrderFeature(string identifier): base(identifier, new PostgresqlMigrator())
    {
    }

    public List<Table> Tables { get; } = new();

    protected override IEnumerable<ISchemaObject> schemaObjects() => Tables;
}

internal class MultiFeatureDatabase: PostgresqlDatabase
{
    private readonly IFeatureSchema[] _features;

    public MultiFeatureDatabase(Npgsql.NpgsqlDataSource dataSource, params IFeatureSchema[] features)
        : base(new DefaultMigrationLogger(), JasperFx.AutoCreate.All, new PostgresqlMigrator(), "multifeature",
            dataSource)
    {
        _features = features;
    }

    public override IFeatureSchema[] BuildFeatureSchemas() => _features;
}
