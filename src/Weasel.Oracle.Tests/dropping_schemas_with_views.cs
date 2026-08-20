using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     weasel#465: <c>DropSchemaAsync</c> queried procedures, functions, tables and sequences, but
///     never <c>all_views</c>. <c>DROP TABLE … CASCADE CONSTRAINTS</c> invalidates a dependent view
///     rather than dropping it, so any view in the schema survived the teardown and the schema was
///     never actually clean.
/// </summary>
/// <remarks>
///     <para>
///         The view here is created with raw SQL rather than through a <c>View</c> schema object,
///         because Oracle view support is still blocked (see weasel#450). That is deliberate: the
///         teardown has to cope with a view whatever created it, including one a user made by hand,
///         and writing the test this way means it is not waiting on the other slice.
///     </para>
///     <para>
///         SQL Server had the identical gap and it went unnoticed until something could create a
///         view (weasel#464). Oracle's is the same trap, found before it was armed.
///     </para>
/// </remarks>
public class dropping_schemas_with_views: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public dropping_schemas_with_views(): base(SchemaName)
    {
    }

    private async Task<bool> viewExistsAsync(string viewName)
    {
        var count = await theConnection
            .CreateCommand(
                $"SELECT COUNT(*) FROM all_views WHERE owner = '{SchemaName}' AND view_name = '{viewName.ToUpperInvariant()}'")
            .ExecuteScalarAsync();

        return Convert.ToInt32(count) > 0;
    }

    [Fact]
    public async Task a_view_does_not_survive_dropping_its_schema()
    {
        await ResetSchema();

        await theConnection
            .CreateCommand($"CREATE TABLE {SchemaName}.teardown_src (id NUMBER PRIMARY KEY, name VARCHAR2(50))")
            .ExecuteNonQueryAsync();

        await theConnection
            .CreateCommand(
                $"CREATE VIEW {SchemaName}.teardown_view AS SELECT id, name FROM {SchemaName}.teardown_src")
            .ExecuteNonQueryAsync();

        (await viewExistsAsync("teardown_view")).ShouldBeTrue();

        await theConnection.DropSchemaAsync(SchemaName);

        (await viewExistsAsync("teardown_view")).ShouldBeFalse();
    }

    /// <summary>
    ///     A view over another view: the drop order has to hold for a chain, not just one level.
    /// </summary>
    [Fact]
    public async Task a_view_over_a_view_does_not_survive_either()
    {
        await ResetSchema();

        await theConnection
            .CreateCommand($"CREATE TABLE {SchemaName}.chain_src (id NUMBER PRIMARY KEY)")
            .ExecuteNonQueryAsync();

        await theConnection
            .CreateCommand($"CREATE VIEW {SchemaName}.chain_first AS SELECT id FROM {SchemaName}.chain_src")
            .ExecuteNonQueryAsync();

        await theConnection
            .CreateCommand($"CREATE VIEW {SchemaName}.chain_second AS SELECT id FROM {SchemaName}.chain_first")
            .ExecuteNonQueryAsync();

        await theConnection.DropSchemaAsync(SchemaName);

        (await viewExistsAsync("chain_first")).ShouldBeFalse();
        (await viewExistsAsync("chain_second")).ShouldBeFalse();
    }
}
