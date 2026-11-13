using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     <c>racing_create_or_alter</c> failed twice running on Firebird 3 in CI (weasel#823's run, 2026-11-13)
///     with <c>unsuccessful metadata update / CREATE OR ALTER VIEW RACE_VIEW failed / BLOB not found</c>:
///     the losing <c>CREATE OR ALTER</c> itself read a view source BLOB that the winning rewrite had just
///     orphaned. That is a lost race like the others <see cref="FirebirdMigrator.IsCatalogConflict" />
///     retries, but it was not classified as one, so the applier failed instead of running the statement
///     again. It would not reproduce on a fast local machine (eight runs against Firebird 3, none
///     failed), so the rule is asserted on the error numbers, which is how the server reports it.
/// </summary>
public class a_create_or_alter_that_raced_a_rewrite
{
    private const int UnsuccessfulMetadataUpdate = 335544351;

    [Fact]
    public void blob_not_found_inside_a_failed_ddl_statement_is_a_lost_race()
    {
        FirebirdMigrator.IsDdlThatRacedARewrite([UnsuccessfulMetadataUpdate, FirebirdMigrator.BlobNotFound])
            .ShouldBeTrue();
    }

    [Fact]
    public void the_order_and_any_numbers_between_them_do_not_matter()
    {
        // The server puts the statement's own "CREATE OR ALTER VIEW ... failed" between the two
        FirebirdMigrator.IsDdlThatRacedARewrite([UnsuccessfulMetadataUpdate, 336397267, FirebirdMigrator.BlobNotFound])
            .ShouldBeTrue();
        FirebirdMigrator.IsDdlThatRacedARewrite([FirebirdMigrator.BlobNotFound, UnsuccessfulMetadataUpdate])
            .ShouldBeTrue();
    }

    /// <summary>
    ///     A raced catalog read (weasel#723) is answered by reading again, never by re-running DDL.
    /// </summary>
    [Fact]
    public void blob_not_found_on_its_own_is_a_raced_read_not_a_lost_statement()
    {
        FirebirdMigrator.IsDdlThatRacedARewrite([FirebirdMigrator.BlobNotFound]).ShouldBeFalse();
    }

    /// <summary>
    ///     335544351 heads every failed DDL statement, a name already taken as much as a lost race.
    /// </summary>
    [Fact]
    public void an_unsuccessful_metadata_update_on_its_own_is_not_retried()
    {
        FirebirdMigrator.IsDdlThatRacedARewrite([UnsuccessfulMetadataUpdate]).ShouldBeFalse();
        FirebirdMigrator.IsDdlThatRacedARewrite([UnsuccessfulMetadataUpdate, 335544453]).ShouldBeFalse();
    }
}
