using Microsoft.Data.Sqlite;
using Weasel.Core;
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;
using Weasel.Sqlite.Views;

namespace DocSamples;

public class SqliteSamples
{
    // === index.md samples ===

    public async Task sqlite_quick_example()
    {
        #region sample_sqlite_quick_example
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);

        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();
        #endregion
    }

    // === tables.md samples ===

    public void sqlite_create_table()
    {
        #region sample_sqlite_create_table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull();
        table.AddColumn("settings", "TEXT"); // raw type
        #endregion
    }

    public void sqlite_autoincrement()
    {
        #region sample_sqlite_autoincrement
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        #endregion
    }

    public void sqlite_generated_columns()
    {
        #region sample_sqlite_generated_columns
        var table = new Table("users");
        table.AddColumn("email_domain", "TEXT")
            .GeneratedAs("substr(email, instr(email, '@') + 1)", GeneratedColumnType.Stored);
        #endregion
    }

    public void sqlite_foreign_keys()
    {
        #region sample_sqlite_foreign_keys
        var orders = new Table("orders");
        orders.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        orders.AddColumn<int>("user_id").NotNull();

        // Define foreign key referencing users table
        orders.ForeignKeys.Add(new ForeignKey("fk_orders_user")
        {
            ColumnNames = new[] { "user_id" },
            LinkedTable = new SqliteObjectName("users"),
            LinkedNames = new[] { "id" }
        });
        #endregion
    }

    public void sqlite_indexes()
    {
        #region sample_sqlite_indexes
        var table = new Table("users");

        // Unique index
        var emailIdx = new IndexDefinition("idx_email") { IsUnique = true };
        emailIdx.AgainstColumns("email");
        table.Indexes.Add(emailIdx);

        // Expression index on JSON path
        var jsonIdx = new IndexDefinition("idx_settings_theme");
        jsonIdx.ForJsonPath("settings", "$.theme");
        table.Indexes.Add(jsonIdx);

        // Partial index with WHERE clause
        var activeIdx = new IndexDefinition("idx_active_users");
        activeIdx.AgainstColumns("name");
        activeIdx.Predicate = "active = 1";
        table.Indexes.Add(activeIdx);
        #endregion
    }

    public void sqlite_strict_and_without_rowid()
    {
        #region sample_sqlite_strict_and_without_rowid
        var table = new Table("users");
        table.StrictTypes = true;   // CREATE TABLE ... (...) STRICT
        table.WithoutRowId = true;  // CREATE TABLE ... (...) WITHOUT ROWID
        #endregion
    }

    public void sqlite_schema_support()
    {
        #region sample_sqlite_schema_support
        // Temporary table
        var temp = new Table(new SqliteObjectName("temp", "session_data"));
        // DDL: CREATE TABLE IF NOT EXISTS "temp"."session_data" (...)

        // Move existing table definition to temp schema
        var table = new Table("users");
        table.MoveToSchema("temp");
        #endregion
    }

    // === views.md samples ===

    public void sqlite_create_view()
    {
        #region sample_sqlite_create_view
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE active = 1");
        #endregion
    }

    public void sqlite_view_ddl()
    {
        #region sample_sqlite_view_ddl
        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE active = 1");

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        view.WriteCreateStatement(migrator, writer);

        // Output:
        // DROP VIEW IF EXISTS "active_users";
        // CREATE VIEW "active_users" AS SELECT id, name, email FROM users WHERE active = 1;
        #endregion
    }

    public void sqlite_view_schema()
    {
        #region sample_sqlite_view_schema
        // Temporary view (connection-scoped)
        var tempView = new View(
            new SqliteObjectName("temp", "session_summary"),
            "SELECT session_id, COUNT(*) as event_count FROM temp.session_data GROUP BY session_id");

        // DDL: DROP VIEW IF EXISTS "temp"."session_summary";
        // CREATE VIEW "temp"."session_summary" AS SELECT ...
        #endregion
    }

    public void sqlite_complex_views()
    {
        #region sample_sqlite_complex_views
        // Aggregation with JOIN
        var orderSummary = new View("user_order_summary", @"
    SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.id, u.name");

        // JSON extraction
        var productDetails = new View("product_details", @"
    SELECT id, name,
        json_extract(metadata, '$.category') as category,
        json_extract(metadata, '$.price') as price
    FROM products");
        #endregion
    }

    public async Task sqlite_view_delta_detection()
    {
        #region sample_sqlite_view_delta_detection
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();

        var view = new View("active_users",
            "SELECT id, name, email FROM users WHERE active = 1");

        // Check if view exists
        var exists = await view.ExistsInDatabaseAsync(connection);

        // Fetch current definition from sqlite_master
        var existing = await view.FetchExistingAsync(connection);

        // Compare expected vs actual
        var expectedView = view;
        var actualView = existing;
        var delta = new ViewDelta(expectedView, actualView);

        switch (delta.Difference)
        {
            case SchemaPatchDifference.None:
                // View matches expected definition
                break;
            case SchemaPatchDifference.Create:
                // View does not exist yet
                break;
            case SchemaPatchDifference.Update:
                // View SQL changed, will drop and recreate
                break;
        }
        #endregion
    }

    // === json.md samples ===

    public void sqlite_json_columns()
    {
        #region sample_sqlite_json_columns
        var table = new Table("products");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn("metadata", "TEXT"); // JSON column
        #endregion
    }

    public void sqlite_json_expression_indexes()
    {
        #region sample_sqlite_json_expression_indexes
        var table = new Table("products");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn("metadata", "TEXT");

        // Index on a JSON field
        var categoryIdx = new IndexDefinition("idx_products_category");
        categoryIdx.ForJsonPath("metadata", "$.category");
        table.Indexes.Add(categoryIdx);

        // Unique index on a JSON field
        var skuIdx = new IndexDefinition("idx_products_sku") { IsUnique = true };
        skuIdx.ForJsonPath("metadata", "$.sku");
        table.Indexes.Add(skuIdx);
        #endregion
    }

    public void sqlite_json_views()
    {
        #region sample_sqlite_json_views
        var view = new View("product_details", @"
    SELECT id, name,
        json_extract(metadata, '$.category') as category,
        json_extract(metadata, '$.price') as price,
        json_extract(metadata, '$.in_stock') as in_stock
    FROM products");
        #endregion
    }

    public async Task sqlite_json_full_example()
    {
        #region sample_sqlite_json_full_example
        var table = new Table("events");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("type").NotNull();
        table.AddColumn("payload", "TEXT"); // JSON data
        table.AddColumn<string>("created_at").NotNull();

        // Index for filtering events by JSON field
        var idx = new IndexDefinition("idx_events_source");
        idx.ForJsonPath("payload", "$.source");
        table.Indexes.Add(idx);

        // Generate and execute DDL
        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);

        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();
        #endregion
    }

    // === pragmas.md samples ===

    public async Task sqlite_pragma_high_performance()
    {
        #region sample_sqlite_pragma_high_performance
        var settings = SqlitePragmaSettings.HighPerformance;
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        await settings.ApplyToConnectionAsync(connection);
        #endregion
    }

    public async Task sqlite_pragma_high_safety()
    {
        #region sample_sqlite_pragma_high_safety
        var settings = SqlitePragmaSettings.HighSafety;
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        await settings.ApplyToConnectionAsync(connection);
        #endregion
    }

    public async Task sqlite_pragma_custom_configuration()
    {
        #region sample_sqlite_pragma_custom_configuration
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.WAL,
            Synchronous = SynchronousMode.NORMAL,
            CacheSize = -32000, // 32MB
            ForeignKeys = true,
            BusyTimeout = 5000,
            WalAutoCheckpoint = 1000
        };
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        await settings.ApplyToConnectionAsync(connection);
        #endregion
    }

    public async Task sqlite_pragma_apply_existing_connection()
    {
        #region sample_sqlite_pragma_apply_existing_connection
        var settings = SqlitePragmaSettings.Default;
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        await settings.ApplyToConnectionAsync(connection);
        #endregion
    }

    public void sqlite_pragma_sql_script()
    {
        #region sample_sqlite_pragma_sql_script
        var settings = SqlitePragmaSettings.Default;
        Console.WriteLine(settings.ToSqlScript());

        // -- SQLite PRAGMA Settings
        // PRAGMA journal_mode = WAL;
        // PRAGMA synchronous = NORMAL;
        // PRAGMA cache_size = -64000;
        // ...
        #endregion
    }

    // === helper.md samples ===

    public async Task sqlite_helper_basic_connection()
    {
        #region sample_sqlite_helper_basic_connection
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();

        // Apply default PRAGMA settings (WAL mode, NORMAL sync, 64MB cache, foreign keys enabled)
        await SqlitePragmaSettings.Default.ApplyToConnectionAsync(connection);
        #endregion
    }

    public async Task sqlite_helper_custom_pragmas()
    {
        #region sample_sqlite_helper_custom_pragmas
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.WAL,
            ForeignKeys = true,
            CacheSize = -64000 // 64MB
        };
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        await settings.ApplyToConnectionAsync(connection);
        #endregion
    }

    public async Task sqlite_helper_presets()
    {
        #region sample_sqlite_helper_presets
        // High performance (reduced safety)
        var highPerfConn = new SqliteConnection("Data Source=myapp.db");
        await highPerfConn.OpenAsync();
        await SqlitePragmaSettings.HighPerformance.ApplyToConnectionAsync(highPerfConn);

        // High safety (maximum durability)
        var highSafetyConn = new SqliteConnection("Data Source=myapp.db");
        await highSafetyConn.OpenAsync();
        await SqlitePragmaSettings.HighSafety.ApplyToConnectionAsync(highSafetyConn);
        #endregion
    }

    public void sqlite_helper_create_migrator()
    {
        #region sample_sqlite_helper_create_migrator
        var migrator = new SqliteMigrator();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();

        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        Console.WriteLine(writer.ToString());
        #endregion
    }

    public void sqlite_connection_string_examples()
    {
        #region sample_sqlite_connection_string_examples
        // In-memory database (lost when connection closes)
        var inMemory = "Data Source=:memory:";

        // File-based database
        var fileBased = "Data Source=myapp.db";

        // Shared cache for multiple connections to the same in-memory database
        var sharedCache = "Data Source=myapp;Mode=Memory;Cache=Shared";

        // Read-only access
        var readOnly = "Data Source=myapp.db;Mode=ReadOnly";
        #endregion
    }

    public async Task sqlite_helper_recommended_usage()
    {
        #region sample_sqlite_helper_recommended_usage
        // Preferred: type-safe PRAGMA configuration
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.WAL,
            Synchronous = SynchronousMode.NORMAL,
            ForeignKeys = true
        };
        var connection = new SqliteConnection("Data Source=myapp.db");
        await connection.OpenAsync();
        await settings.ApplyToConnectionAsync(connection);

        // Avoid: raw PRAGMA statements
        // var cmd = connection.CreateCommand();
        // cmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;";
        #endregion
    }
}
