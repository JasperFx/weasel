using JasperFx;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.CommandLine.Tests;

/// <summary>
///     Groundwork for weasel#431. Applying databases in parallel means several appliers writing at
///     once, and today the migration DDL goes straight to <c>System.Console</c> from inside the apply
///     -- so it would interleave line by line and stop being attributable to any one database. The
///     logger has to be redirectable per database before that can be made safe.
/// </summary>
public class migration_logger_seam
{
    [Fact]
    public void writes_to_the_supplied_writer_instead_of_the_console()
    {
        var writer = new StringWriter();
        var logger = new DefaultMigrationLogger(writer);

        logger.SchemaChange("create table foo ();");

        writer.ToString().Trim().ShouldBe("create table foo ();");
    }

    [Fact]
    public void will_not_accept_a_null_writer()
    {
        Should.Throw<ArgumentNullException>(() => new DefaultMigrationLogger(null!));
    }

    /// <summary>
    ///     Every provider's <c>executeDelta</c> does <c>if (logger is DefaultMigrationLogger) throw;</c>
    ///     so that a failed migration statement surfaces with its original stack trace rather than the
    ///     reset one <c>OnFailure</c> would produce. A buffering logger has to stay on the right side of
    ///     that check, which is why buffering is a constructor overload rather than a new type.
    /// </summary>
    [Fact]
    public void a_buffering_logger_is_still_a_DefaultMigrationLogger()
    {
        new DefaultMigrationLogger(new StringWriter())
            .ShouldBeOfType<DefaultMigrationLogger>();
    }

    [Fact]
    public void the_logger_is_swappable_after_construction()
    {
        var database = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "swappable");
        var replacement = new DefaultMigrationLogger(new StringWriter());

        database.ShouldBeAssignableTo<IDatabaseWithMigrationLogger>();

        database.MigrationLogger = replacement;
        database.MigrationLogger.ShouldBeSameAs(replacement);
    }

    [Fact]
    public void will_not_accept_a_null_logger()
    {
        var database = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "not_null");

        Should.Throw<ArgumentNullException>(() => database.MigrationLogger = null!);
    }
}

/// <summary>
///     The seam is only worth anything if the DDL an actual apply emits follows it, so this drives a
///     real migration against a real database rather than calling the logger directly.
/// </summary>
[Collection("integration")]
public class migration_logger_seam_end_to_end: IntegrationContext
{
    [Fact]
    public async Task applied_ddl_goes_to_the_swapped_in_logger()
    {
        await DropSchema("logger_seam");

        var buffer = new StringWriter();
        var database = Databases["logger_seam_db"];
        database.Features["seam"].AddTable("logger_seam", "names");
        database.MigrationLogger = new DefaultMigrationLogger(buffer);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var ddl = buffer.ToString();
        ddl.ShouldContain("logger_seam.names");

        // And the database really was migrated -- the DDL was redirected, not swallowed.
        await database.AssertDatabaseMatchesConfigurationAsync();
    }
}
