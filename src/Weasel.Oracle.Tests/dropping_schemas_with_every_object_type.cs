using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     weasel#465: Oracle's teardown enumerates object types by hand, so it falls behind every time
///     a new one becomes creatable. #466 added views after SQL Server was caught with the identical
///     gap. This covers the rest of what an Oracle schema can hold — triggers, packages, synonyms,
///     object types and materialized views — none of which the sweep knew about.
/// </summary>
/// <remarks>
///     <para>
///         These are created with raw SQL rather than through Weasel schema objects, and
///         deliberately so: the teardown has to cope with whatever is in the schema, including
///         objects a user created by hand or another tool left behind. Writing it this way also
///         means the coverage does not wait on #451 / #452 / #453 to add the corresponding
///         <c>ISchemaObject</c>.
///     </para>
///     <para>
///         The materialized view is the interesting one. Oracle lists its container table in
///         <c>all_tables</c> under the same name, and <c>DROP TABLE</c> against it fails with
///         ORA-12083 — so the naive fix of "just add mviews to the list" breaks the table sweep
///         that was already working.
///     </para>
/// </remarks>
public class dropping_schemas_with_every_object_type: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public dropping_schemas_with_every_object_type(): base(SchemaName)
    {
    }

    private async Task<int> countOfAsync(string objectType)
    {
        var count = await theConnection
            .CreateCommand(
                $"SELECT COUNT(*) FROM all_objects WHERE owner = '{SchemaName}' AND object_type = '{objectType}'")
            .ExecuteScalarAsync();

        return Convert.ToInt32(count);
    }

    private Task executeAsync(string sql)
        => theConnection.CreateCommand(sql).ExecuteNonQueryAsync();

    [Fact]
    public async Task a_trigger_does_not_survive_dropping_its_schema()
    {
        await ResetSchema();

        await executeAsync($"CREATE TABLE {SchemaName}.trg_src (id NUMBER PRIMARY KEY)");
        await executeAsync(
            $"CREATE TRIGGER {SchemaName}.trg_probe BEFORE INSERT ON {SchemaName}.trg_src FOR EACH ROW BEGIN NULL; END;");

        (await countOfAsync("TRIGGER")).ShouldBe(1);

        await theConnection.DropSchemaAsync(SchemaName);

        (await countOfAsync("TRIGGER")).ShouldBe(0);
    }

    [Fact]
    public async Task a_package_does_not_survive_dropping_its_schema()
    {
        await ResetSchema();

        await executeAsync($"CREATE PACKAGE {SchemaName}.pkg_probe AS PROCEDURE noop; END;");

        (await countOfAsync("PACKAGE")).ShouldBe(1);

        await theConnection.DropSchemaAsync(SchemaName);

        (await countOfAsync("PACKAGE")).ShouldBe(0);
    }

    [Fact]
    public async Task a_synonym_does_not_survive_dropping_its_schema()
    {
        await ResetSchema();

        await executeAsync($"CREATE TABLE {SchemaName}.syn_src (id NUMBER PRIMARY KEY)");
        await executeAsync($"CREATE SYNONYM {SchemaName}.syn_probe FOR {SchemaName}.syn_src");

        (await countOfAsync("SYNONYM")).ShouldBe(1);

        await theConnection.DropSchemaAsync(SchemaName);

        (await countOfAsync("SYNONYM")).ShouldBe(0);
    }

    [Fact]
    public async Task an_object_type_does_not_survive_dropping_its_schema()
    {
        await ResetSchema();

        await executeAsync($"CREATE TYPE {SchemaName}.typ_probe AS OBJECT (a NUMBER)");

        (await countOfAsync("TYPE")).ShouldBe(1);

        await theConnection.DropSchemaAsync(SchemaName);

        (await countOfAsync("TYPE")).ShouldBe(0);
    }

    /// <summary>
    ///     A type a table column is declared with. <c>DROP TYPE</c> without <c>FORCE</c> fails with
    ///     ORA-02303 while any dependent exists, and the sweep drops tables before types, so the
    ///     dependency is normally gone by then — <c>FORCE</c> is there for the case where it is not.
    /// </summary>
    [Fact]
    public async Task a_type_a_table_depends_on_does_not_survive_either()
    {
        await ResetSchema();

        await executeAsync($"CREATE TYPE {SchemaName}.addr_probe AS OBJECT (street VARCHAR2(50))");
        await executeAsync($"CREATE TABLE {SchemaName}.typed_src (id NUMBER, home {SchemaName}.addr_probe)");

        await theConnection.DropSchemaAsync(SchemaName);

        (await countOfAsync("TYPE")).ShouldBe(0);
        (await countOfAsync("TABLE")).ShouldBe(0);
    }

    [Fact]
    public async Task a_materialized_view_does_not_survive_dropping_its_schema()
    {
        await ResetSchema();

        await executeAsync($"CREATE TABLE {SchemaName}.mv_src (id NUMBER PRIMARY KEY, qty NUMBER)");
        await executeAsync($"CREATE MATERIALIZED VIEW {SchemaName}.mv_probe AS SELECT id, qty FROM {SchemaName}.mv_src");

        (await countOfAsync("MATERIALIZED VIEW")).ShouldBe(1);

        await theConnection.DropSchemaAsync(SchemaName);

        (await countOfAsync("MATERIALIZED VIEW")).ShouldBe(0);
        (await countOfAsync("TABLE")).ShouldBe(0);
    }

    /// <summary>
    ///     The whole point of the issue: after a teardown the schema holds nothing, whatever was in
    ///     it. One object of every kind at once, because the drop order only has to hold when they
    ///     are all present together.
    /// </summary>
    [Fact]
    public async Task nothing_at_all_survives_dropping_the_schema()
    {
        await ResetSchema();

        await executeAsync($"CREATE TABLE {SchemaName}.all_src (id NUMBER PRIMARY KEY, qty NUMBER)");
        await executeAsync($"CREATE INDEX {SchemaName}.all_idx ON {SchemaName}.all_src (qty)");
        await executeAsync($"CREATE SEQUENCE {SchemaName}.all_seq");
        await executeAsync($"CREATE VIEW {SchemaName}.all_view AS SELECT id FROM {SchemaName}.all_src");
        await executeAsync(
            $"CREATE MATERIALIZED VIEW {SchemaName}.all_mv AS SELECT id, qty FROM {SchemaName}.all_src");
        await executeAsync(
            $"CREATE TRIGGER {SchemaName}.all_trg BEFORE INSERT ON {SchemaName}.all_src FOR EACH ROW BEGIN NULL; END;");
        await executeAsync($"CREATE PACKAGE {SchemaName}.all_pkg AS PROCEDURE noop; END;");
        await executeAsync($"CREATE PROCEDURE {SchemaName}.all_proc AS BEGIN NULL; END;");
        await executeAsync($"CREATE FUNCTION {SchemaName}.all_fn RETURN NUMBER AS BEGIN RETURN 1; END;");
        await executeAsync($"CREATE SYNONYM {SchemaName}.all_syn FOR {SchemaName}.all_src");
        await executeAsync($"CREATE TYPE {SchemaName}.all_typ AS OBJECT (a NUMBER)");

        await theConnection.DropSchemaAsync(SchemaName);

        var leftovers = await theConnection
            .CreateCommand(
                $"SELECT object_type || ' ' || object_name FROM all_objects WHERE owner = '{SchemaName}' ORDER BY 1")
            .FetchListAsync<string>();

        leftovers.ShouldBeEmpty();
    }
}
