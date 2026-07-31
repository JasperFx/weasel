using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;
using Shouldly;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
/// weasel#416 — partition <b>bound values</b> are interpolated into single-quoted SQL literals. Partition
/// <em>suffixes</em> are safe (<see cref="ListPartition.SanitizeSuffix"/> character-whitelists them to
/// <c>[a-z0-9_]</c>), but the values were not escaped, and consumers use tenant ids as partition values.
///
/// <para>
/// Two defects, both in <see cref="PartitionExtensions.FormatSqlValue{T}"/>: an embedded single quote was not
/// doubled, and any string that merely started and ended with a quote was returned completely verbatim — so a
/// value that happened to be quote-wrapped skipped escaping altogether. That second branch existed to make the
/// function idempotent; "is this already a literal?" cannot be decided from the shape of untrusted input, so it
/// is gone and callers holding real literals now say so explicitly via <c>AddPartitionWithSqlLiterals</c> /
/// <c>AddRangeWithSqlLiterals</c>.
/// </para>
/// </summary>
public class partition_value_literal_escaping
{
    [Fact]
    public void doubles_an_embedded_single_quote()
    {
        "O'Brien".FormatSqlValue().ShouldBe("'O''Brien'");
    }

    /// <summary>
    /// The removed short-circuit. A raw value that happens to begin and end with a quote must still be
    /// escaped — this is the shape an attacker picks precisely because it used to pass through untouched.
    /// </summary>
    [Fact]
    public void escapes_a_value_that_merely_looks_like_a_literal()
    {
        "'); create table public.marten_marker(i int); --'"
            .FormatSqlValue()
            .ShouldBe("'''); create table public.marten_marker(i int); --'''");
    }

    [Fact]
    public void still_formats_ordinary_values_unchanged()
    {
        "tenant-a".FormatSqlValue().ShouldBe("'tenant-a'");
        20.FormatSqlValue().ShouldBe("20");
        true.FormatSqlValue().ShouldBe("true");
    }

    /// <summary>
    /// The generated DDL must contain the payload as inert text inside one literal, and must not contain the
    /// statement separator that would turn it into a second statement.
    /// </summary>
    [Fact]
    public void injected_value_stays_inside_the_bound_literal()
    {
        var table = new Table(new DbObjectName("partitions", "people"));
        var partitioning = new ListPartitioning { Columns = ["tenant_id"] }
            .AddPartition("evil", "x'); create table public.marten_marker(i int); --");

        var writer = new StringWriter();
        ((IPartition)partitioning.Partitions.Single()).WriteCreateStatement(writer, table);
        var ddl = writer.ToString();

        ddl.ShouldContain("for values in ('x''); create table public.marten_marker(i int); --')");
        ddl.ShouldNotContain("'); create table public.marten_marker(i int); --')\n");
    }
}

/// <summary>
/// End-to-end half of weasel#416: a partition value containing a single quote has to survive a real round trip
/// through <see cref="ManagedListPartitions"/> — create, read back, and drop — without executing anything and
/// without drifting on the next migration.
/// </summary>
[Collection("managed_lists")]
public class partition_values_with_quotes_round_trip : IntegrationContext
{
    public partition_values_with_quotes_round_trip() : base("managed_lists")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private const string Payload = "x'); create table public.weasel_416_marker(i int); --";

    [Fact]
    public async Task quoted_value_provisions_and_does_not_execute_injected_sql()
    {
        await DropMarkerAsync();

        var database = new ManagedListDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var partitions = new Dictionary<string, string> { { Payload, "evil" }, { "O'Brien", "obrien" } };
        await database.Partitions.AddPartitionToAllTables(
            NullLogger.Instance, database, partitions, CancellationToken.None);

        (await MarkerExistsAsync()).ShouldBeFalse("the payload must stay inert inside the bound literal");

        // The partitions really were created, and the values round-tripped intact through pg_get_expr.
        var tables = await database.FetchExistingTablesAsync();
        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var listing = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        listing.Partitions.Select(x => x.Suffix).ShouldContain("evil");
        listing.Partitions.Select(x => x.Suffix).ShouldContain("obrien");

        // ...and a second apply must be a no-op rather than a spurious rebuild, which is what would happen if
        // the declared and read-back literals disagreed about escaping.
        await Should.NotThrowAsync(() => database.ApplyAllConfiguredChangesToDatabaseAsync());
        (await MarkerExistsAsync()).ShouldBeFalse("nor on a re-apply");
    }

    [Fact]
    public async Task quoted_value_can_be_dropped()
    {
        await DropMarkerAsync();

        var database = new ManagedListDatabase();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Two values sharing one suffix, so removing one takes the DETACH/ATTACH rebind path that re-emits the
        // survivors' literals — the drop-side mirror of the create-side escaping.
        var partitions = new Dictionary<string, string> { { Payload, "shared" }, { "O'Brien", "shared" } };
        await database.Partitions.AddPartitionToAllTables(
            NullLogger.Instance, database, partitions, CancellationToken.None);

        await database.Partitions.DropPartitionFromAllTablesForValue(
            database, NullLogger.Instance, Payload, CancellationToken.None);

        (await MarkerExistsAsync()).ShouldBeFalse("the drop/rebind path must not execute injected SQL either");
        database.Partitions.Partitions.ContainsKey(Payload).ShouldBeFalse();
        database.Partitions.Partitions.ContainsKey("O'Brien").ShouldBeTrue("the co-tenant must survive");
    }

    private static async Task<bool> MarkerExistsAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select to_regclass('public.weasel_416_marker') is not null";
        return (bool)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task DropMarkerAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "drop table if exists public.weasel_416_marker";
        await cmd.ExecuteNonQueryAsync();
    }
}
