using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Packages;
using Xunit;

namespace Weasel.Oracle.Tests.Packages;

/// <summary>
///     Oracle packages (weasel#453). The spec and the body are two objects with separate validity,
///     which is why packages did not fit <c>FunctionBase</c>'s single-body shape.
/// </summary>
[Collection("integration")]
public class PackageTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public PackageTests(): base(SchemaName)
    {
    }

    private static Package NewPackage(string constant = "one") => new(
        $"{SchemaName}.pkg_probe",
        $@"CREATE OR REPLACE PACKAGE {SchemaName}.pkg_probe AS
    FUNCTION label RETURN VARCHAR2;
END pkg_probe;",
        $@"CREATE OR REPLACE PACKAGE BODY {SchemaName}.pkg_probe AS
    FUNCTION label RETURN VARCHAR2 IS
    BEGIN
        RETURN '{constant}';
    END label;
END pkg_probe;");

    private static Package SpecOnly() => new(
        $"{SchemaName}.pkg_constants",
        $@"CREATE OR REPLACE PACKAGE {SchemaName}.pkg_constants AS
    max_retries CONSTANT PLS_INTEGER := 3;
END pkg_constants;");

    [Fact]
    public void the_create_or_replace_prefix_and_the_schema_qualifier_are_stripped()
    {
        Package.Strip("CREATE OR REPLACE PACKAGE WEASEL.p AS END;", "WEASEL")
            .ShouldStartWith("PACKAGE p");
    }

    /// <summary>
    ///     The <c>BODY</c> keyword sits between <c>PACKAGE</c> and the name, so stripping the schema
    ///     has to survive it.
    /// </summary>
    [Fact]
    public void stripping_a_package_body_keeps_the_body_keyword()
    {
        Package.Strip("CREATE OR REPLACE PACKAGE BODY WEASEL.p AS END;", "WEASEL")
            .ShouldStartWith("PACKAGE BODY p");
    }

    [Fact]
    public async Task a_package_round_trips_and_reports_no_delta()
    {
        await ResetSchema();

        var package = NewPackage();
        await package.ApplyChangesAsync(theConnection);

        (await package.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await package.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_package_does_not_report_permanent_drift()
    {
        await ResetSchema();
        await NewPackage().ApplyChangesAsync(theConnection);

        (await NewPackage().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await NewPackage().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_package_actually_runs()
    {
        await ResetSchema();
        await NewPackage().ApplyChangesAsync(theConnection);

        var label = await theConnection
            .CreateCommand($"SELECT {SchemaName}.pkg_probe.label FROM dual")
            .ExecuteScalarAsync();

        label.ShouldBe("one");
    }

    /// <summary>
    ///     A change to the body alone, with the spec untouched — the case a single-body model could
    ///     not have expressed.
    /// </summary>
    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await NewPackage().ApplyChangesAsync(theConnection);

        var changed = NewPackage("two");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var label = await theConnection
            .CreateCommand($"SELECT {SchemaName}.pkg_probe.label FROM dual")
            .ExecuteScalarAsync();

        label.ShouldBe("two");
    }

    /// <summary>
    ///     A spec with no body is legal Oracle — it is how shared constants and types are declared —
    ///     so <c>Body</c> is optional rather than required.
    /// </summary>
    [Fact]
    public async Task a_spec_only_package_round_trips()
    {
        await ResetSchema();

        var package = SpecOnly();
        await package.ApplyChangesAsync(theConnection);

        (await package.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await package.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var (_, body) = await package.FetchExistingAsync(theConnection);
        body.ShouldBeNull();
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_packages_with_it()
    {
        await ResetSchema();
        await NewPackage().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewPackage().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
