using JasperFx;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     Regression test for the original Marten Bug 983
///     (<c>Bug_983_autocreate_none_is_disabling_schema_validation</c>) — ported to
///     Weasel as part of #278 because the underlying
///     <see cref="IDatabase.AssertDatabaseMatchesConfigurationAsync" /> logic now
///     lives in <c>Weasel.Core.Migrations</c>, not Marten.
///     <para>
///     The original bug: <c>AutoCreate.None</c> was short-circuiting schema
///     validation, so a database missing tables that the application configured
///     silently passed <c>AssertDatabaseMatchesConfigurationAsync</c>. The fix
///     made the assertion run regardless of <c>AutoCreate</c> — the autocreate
///     setting controls whether <c>ApplyAllConfiguredChangesToDatabaseAsync</c>
///     will <em>apply</em> changes, not whether the assert detects them.
///     </para>
/// </summary>
[Collection("bug983")]
public class Bug983_AutoCreateNoneStillValidatesSchema: IntegrationContext
{
    public Bug983_AutoCreateNoneStillValidatesSchema(): base("bug983")
    {
    }

    [Fact]
    public async Task assert_throws_when_table_is_missing_even_with_autocreate_none()
    {
        // Make sure the schema is empty so the configured table is genuinely missing.
        await ResetSchema();

        // Configure a database with one table that we deliberately don't apply, and
        // turn auto-create off. Pre-fix, AutoCreate.None caused the assert to be a
        // silent no-op; post-fix, the assert must still detect the missing table.
        var db = new DatabaseWithTables("bug983", theDataSource, AutoCreate.None);
        var table = db.AddTable(new PostgresqlObjectName(SchemaName, "documents"));
        table.AddPrimaryKeyColumn("id", typeof(int));

        await Should.ThrowAsync<DatabaseValidationException>(
            () => db.AssertDatabaseMatchesConfigurationAsync());
    }
}
