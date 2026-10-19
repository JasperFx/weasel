using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     weasel#723. Firebird keeps a view's, a routine's and a trigger's source in a BLOB, and a catalog
///     row carries only the BLOB's id -- the client fetches the blob itself in a later round trip. A
///     concurrent <c>CREATE OR ALTER</c> replaces the record version the id came from, and the blob can
///     be collected before that fetch, so the read fails with "BLOB not found" for something no reader
///     did wrong.
/// </summary>
/// <remarks>
///     <para>
///         It surfaced as an intermittent failure of
///         <c>racing_create_or_alter.racing_appliers_all_succeed_and_the_objects_converge</c> on Firebird
///         5, which passed on the rerun and on the other three legs. The interleave is cheap to force,
///         though, so none of this is left to a race: read the row, let another attachment rewrite the
///         view, then materialize.
///     </para>
///     <para>
///         The classification is the point. The failure is on the <em>read</em> side, which is why
///         Firebird's statement retry -- <see cref="FirebirdMigrator.IsCatalogConflict" />, the five
///         attempts a losing <c>CREATE OR ALTER</c> gets -- never saw it: the error cannot come from a
///         DDL statement at all. It is answered instead by
///         <c>Migrator.IsConcurrentIntrospectionFailure</c>, which makes
///         <c>SchemaMigration.DetermineAsync</c> read the whole set again.
///     </para>
/// </remarks>
public class a_catalog_read_that_raced_a_rewrite: IntegrationContext
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        await ExecuteAsync("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, note VARCHAR(40));");
        await ExecuteAsync("CREATE OR ALTER VIEW v AS SELECT id, note FROM t;");
    }

    /// <summary>
    ///     Force the interleave and keep the exception the server really produced, rather than building
    ///     an FbException by hand and asserting against a number nothing confirmed --
    ///     <c>FirebirdClient</c> does not let one be constructed anyway.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Firebird 4 and 5 only. On Firebird 3 the blob stays reachable however the interleave is
    ///         arranged: measured up to forty rewrites, with a delay and a third attachment reading the
    ///         catalog to prod a cooperative collector, and it never once failed. Its collector simply
    ///         does not take that version away under an open read.
    ///     </para>
    ///     <para>
    ///         So the three tests that need the error skip on Firebird 3, named, rather than passing
    ///         quietly on an interleave that proved nothing -- and nothing is lost by it: the
    ///         classification under test is an error-number comparison with no version in it, and the
    ///         negative case below runs everywhere. If a Firebird 3 server does produce this error under
    ///         some load, the fix handles it the same way.
    ///     </para>
    /// </remarks>
    private async Task<FbException> readSourceAcrossARewriteAsync()
    {
        if (ServerVersion.Major < 4)
        {
            Assert.Skip($"Firebird {ServerVersion} does not collect the blob under an open read");
        }

        // Two rewrites: one leaves the blob the reader's id points at still reachable, the second
        // orphans it. Measured against Firebird 4 and 5 -- one is not enough, two always is.
        for (var rewrites = 1; rewrites <= 6; rewrites++)
        {
            await using var readerConn = await OpenConnectionAsync();
            await using var cmd = readerConn.CreateCommand();
            cmd.CommandText = "SELECT RDB$VIEW_SOURCE FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'V'";
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            // The row, with its BLOB id. Not the blob.
            (await reader.ReadAsync(ct)).ShouldBeTrue();

            await using (var writerConn = await OpenConnectionAsync())
            {
                for (var i = 0; i < rewrites; i++)
                {
                    await using var rewrite = writerConn.CreateCommand();
                    rewrite.CommandText = "CREATE OR ALTER VIEW v AS SELECT id, note FROM t";
                    await rewrite.ExecuteNonQueryAsync(ct);
                }
            }

            try
            {
                _ = reader.GetValue(0);
            }
            catch (FbException e)
            {
                return e;
            }
        }

        throw new InvalidOperationException(
            $"Firebird {ServerVersion} did not collect the blob under the read, so this test proves "
            + "nothing. If this version no longer behaves as 4 and 5 did, skip it here as 3 is skipped "
            + "-- but the classification still needs covering on some version that does.");
    }

    [Fact]
    public async Task the_server_really_produces_blob_not_found()
    {
        var failure = await readSourceAcrossARewriteAsync();

        FirebirdMigrator.HasErrorNumber(failure, FirebirdMigrator.BlobNotFound)
            .ShouldBeTrue($"expected {FirebirdMigrator.BlobNotFound}, got: {failure.Message}");
    }

    [Fact]
    public async Task it_is_classified_as_a_raced_read_so_the_introspection_is_read_again()
    {
        var failure = await readSourceAcrossARewriteAsync();

        new FirebirdMigrator().IsConcurrentIntrospectionFailure(failure).ShouldBeTrue(failure.Message);
    }

    /// <summary>
    ///     And not as a statement conflict, which would have a losing applier re-run its DDL in answer
    ///     to a failed read.
    /// </summary>
    [Fact]
    public async Task it_is_not_a_catalog_conflict()
    {
        var failure = await readSourceAcrossARewriteAsync();

        FirebirdMigrator.IsCatalogConflict(failure).ShouldBeFalse(failure.Message);
    }

    /// <summary>
    ///     The classification is by error number, not by anything looser: an ordinary failed DDL is
    ///     still a real failure.
    /// </summary>
    [Fact]
    public async Task an_ordinary_ddl_failure_is_not_a_raced_read()
    {
        var failure = await Should.ThrowAsync<FbException>(async () =>
        {
            await using var cmd = theConnection.CreateCommand();
            cmd.CommandText = "CREATE VIEW v AS SELECT id FROM t";
            await cmd.ExecuteNonQueryAsync(ct);
        });

        new FirebirdMigrator().IsConcurrentIntrospectionFailure(failure).ShouldBeFalse(failure.Message);
    }
}
