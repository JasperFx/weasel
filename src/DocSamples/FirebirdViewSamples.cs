using FirebirdSql.Data.FirebirdClient;
using Weasel.Firebird;
using Weasel.Firebird.Views;

namespace DocSamples;

/// <summary>
///     Samples for the Firebird page on views.
/// </summary>
public class FirebirdViewSamples
{
    // views.md samples

    public void firebird_define_view()
    {
        #region sample_firebird_define_view
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active");
        #endregion
    }

    public void firebird_view_ddl()
    {
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active");

        #region sample_firebird_view_ddl
        var migrator = new FirebirdMigrator();
        var writer = new StringWriter();

        view.WriteCreateStatement(migrator, writer);
        // CREATE OR ALTER VIEW active_users AS SELECT id, name, email FROM users WHERE is_active;

        view.WriteDropStatement(migrator, writer);
        // An EXECUTE BLOCK that drops the view only while it exists
        #endregion
    }

    public async Task firebird_view_delta(string connectionString)
    {
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active");

        #region sample_firebird_view_delta
        await using var conn = new FbConnection(connectionString);
        await conn.OpenAsync();

        var delta = await view.FindDeltaAsync(conn);
        // Create, Update or None

        await view.ApplyChangesAsync(conn);
        #endregion
    }
}
