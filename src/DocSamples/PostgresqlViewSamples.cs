using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Views;

namespace DocSamples;

public class PostgresqlViewSamples
{
    public void create_standard_view()
    {
        #region sample_pg_create_standard_view
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active = true");
        #endregion
    }

    public void view_in_schema()
    {
        #region sample_pg_view_in_schema
        var view = new View(
            new DbObjectName("reporting", "monthly_totals"),
            "SELECT date_trunc('month', created_at) AS month, SUM(amount) FROM orders GROUP BY 1");
        #endregion
    }

    public void move_view_to_schema()
    {
        #region sample_pg_move_view_to_schema
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active = true");
        view.MoveToSchema("analytics");
        #endregion
    }

    public void create_materialized_view()
    {
        #region sample_pg_create_materialized_view
        var matView = new MaterializedView("product_stats",
            "SELECT product_id, COUNT(*) as order_count, SUM(amount) as total FROM orders GROUP BY product_id");

        // Optionally specify a custom access method (e.g., columnar)
        matView.UseAccessMethod("columnar");
        #endregion
    }

    public async Task view_exists_check()
    {
        #region sample_pg_view_exists_check
        var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active = true");

        await using var conn = dataSource.CreateConnection();
        await conn.OpenAsync();

        // Check existence
        bool exists = await view.ExistsInDatabaseAsync(conn);
        #endregion
    }

    public void view_generate_ddl()
    {
        #region sample_pg_view_generate_ddl
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active = true");

        var migrator = new PostgresqlMigrator();
        var writer = new StringWriter();

        view.WriteCreateStatement(migrator, writer);
        // DROP VIEW IF EXISTS public.active_users;
        // CREATE VIEW public.active_users AS SELECT id, name, email FROM users WHERE is_active = true;
        #endregion
    }

    public void view_to_basic_sql()
    {
        #region sample_pg_view_to_basic_sql
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE is_active = true");

        string sql = view.ToBasicCreateViewSql();
        #endregion
    }
}
