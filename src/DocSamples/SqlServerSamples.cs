using Weasel.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Functions;
using Weasel.SqlServer.Procedures;
using Weasel.SqlServer.Tables;
using Microsoft.Data.SqlClient;

namespace DocSamples;

public class SqlServerSamples
{
    // index.md samples

    public void ss_connection_string()
    {
        #region sample_ss_connection_string
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        #endregion
    }

    public void ss_create_migrator()
    {
        #region sample_ss_create_migrator
        var migrator = new SqlServerMigrator();
        #endregion
    }

    public async Task ss_ensure_database_exists()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var migrator = new SqlServerMigrator();

        #region sample_ss_ensure_database_exists
        await using var conn = new SqlConnection(connectionString);
        await migrator.EnsureDatabaseExistsAsync(conn);
        #endregion
    }

    public void ss_schema_management()
    {
        #region sample_ss_schema_management
        var migrator = new SqlServerMigrator();
        var writer = new StringWriter();
        migrator.WriteSchemaCreationSql(new[] { "myschema" }, writer);
        // Generates: IF NOT EXISTS (...) EXEC('CREATE SCHEMA [myschema]');
        #endregion
    }

    // tables.md samples

    public void ss_define_table()
    {
        #region sample_ss_define_table
        var table = new Table("dbo.users");

        table.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull().AddIndex(idx => idx.IsUnique = true);
        table.AddColumn<DateTime>("created_at").DefaultValueByExpression("GETUTCDATE()");
        #endregion
    }

    public void ss_foreign_keys()
    {
        #region sample_ss_foreign_keys
        var orders = new Table("dbo.orders");
        orders.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
        orders.AddColumn<int>("user_id").NotNull()
            .ForeignKeyTo("dbo.users", "id", onDelete: Weasel.SqlServer.CascadeAction.Cascade);
        orders.AddColumn<decimal>("total").NotNull();
        #endregion
    }

    public void ss_indexes()
    {
        var table = new Table("dbo.users");

        #region sample_ss_indexes
        var index = new IndexDefinition("ix_users_email")
        {
            Columns = new[] { "email" },
            IsUnique = true,
            IsClustered = false,
            Predicate = "email IS NOT NULL"  // filtered index
        };
        table.Indexes.Add(index);
        #endregion
    }

    public async Task ss_delta_detection()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var table = new Table("dbo.users");

        #region sample_ss_delta_detection
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        var delta = await table.FindDeltaAsync(conn);
        // delta.Difference is None, Create, Update, or Recreate
        #endregion
    }

    public void ss_generate_ddl()
    {
        var table = new Table("dbo.users");

        #region sample_ss_generate_ddl
        var migrator = new SqlServerMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        Console.WriteLine(writer.ToString());
        #endregion
    }

    // procedures.md samples

    public void ss_define_stored_procedure()
    {
        #region sample_ss_define_stored_procedure
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.usp_get_active_users");

        var proc = new StoredProcedure(identifier, @"
CREATE PROCEDURE dbo.usp_get_active_users
    @MinAge INT = 18
AS
BEGIN
    SET NOCOUNT ON;
    SELECT Id, Name, Email
    FROM dbo.users
    WHERE Active = 1 AND Age >= @MinAge;
END;
");
        #endregion
    }

    public void ss_procedure_ddl()
    {
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.usp_get_active_users");
        var proc = new StoredProcedure(identifier, "CREATE PROCEDURE dbo.usp_get_active_users AS BEGIN SELECT 1; END;");

        #region sample_ss_procedure_ddl
        var migrator = new SqlServerMigrator();
        var writer = new StringWriter();

        // CREATE PROCEDURE
        proc.WriteCreateStatement(migrator, writer);

        // CREATE OR ALTER PROCEDURE (for updates)
        proc.WriteCreateOrAlterStatement(migrator, writer);

        // DROP PROCEDURE IF EXISTS
        proc.WriteDropStatement(migrator, writer);
        #endregion
    }

    public async Task ss_procedure_delta_detection()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.usp_get_active_users");
        var proc = new StoredProcedure(identifier, "CREATE PROCEDURE dbo.usp_get_active_users AS BEGIN SELECT 1; END;");

        #region sample_ss_procedure_delta_detection
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        var delta = await proc.FindDeltaAsync(conn);
        if (delta.Difference == SchemaPatchDifference.Create)
        {
            // Procedure does not exist yet
        }
        else if (delta.Difference == SchemaPatchDifference.Update)
        {
            // Procedure body has changed
        }
        #endregion
    }

    public async Task ss_procedure_fetch_existing()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.usp_get_active_users");
        var proc = new StoredProcedure(identifier, "CREATE PROCEDURE dbo.usp_get_active_users AS BEGIN SELECT 1; END;");

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        #region sample_ss_procedure_fetch_existing
        var existing = await proc.FetchExistingAsync(conn);
        if (existing != null)
        {
            // existing contains the current procedure body from the database
        }
        #endregion
    }

    // functions.md samples

    public void ss_function_from_sql()
    {
        #region sample_ss_function_from_sql
        var fn = Function.ForSql(@"
CREATE FUNCTION dbo.CalculateDiscount(@Price DECIMAL(10,2), @Rate DECIMAL(5,2))
RETURNS DECIMAL(10,2)
AS
BEGIN
    RETURN @Price * @Rate;
END;
");
        #endregion
    }

    public void ss_function_constructor()
    {
        #region sample_ss_function_constructor
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.GetUserCount");

        var fn = new Function(identifier, @"
CREATE FUNCTION dbo.GetUserCount()
RETURNS INT
AS
BEGIN
    RETURN (SELECT COUNT(*) FROM dbo.users);
END;
");
        #endregion
    }

    public void ss_function_custom_drop()
    {
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.GetUserCount");
        var body = "CREATE FUNCTION dbo.GetUserCount() RETURNS INT AS BEGIN RETURN 0; END;";

        #region sample_ss_function_custom_drop
        var fn = new Function(identifier, body, new[]
        {
            "DROP FUNCTION IF EXISTS dbo.GetUserCount;"
        });
        #endregion
    }

    public async Task ss_function_delta_detection()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.GetUserCount");
        var fn = new Function(identifier, "CREATE FUNCTION dbo.GetUserCount() RETURNS INT AS BEGIN RETURN 0; END;");

        #region sample_ss_function_delta_detection
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        var delta = await fn.FindDeltaAsync(conn);
        // delta.Difference: None, Create, or Update
        #endregion
    }

    public void ss_function_for_removal()
    {
        #region sample_ss_function_for_removal
        var removed = Function.ForRemoval("dbo.ObsoleteFunction");
        #endregion
    }

    // sequences.md samples

    public void ss_define_sequence()
    {
        #region sample_ss_define_sequence
        // Simple sequence starting at 1
        var seq = new Sequence("dbo.order_seq");

        // Sequence with a custom start value
        var seq2 = new Sequence(
            DbObjectName.Parse(SqlServerProvider.Instance, "dbo.invoice_seq"),
            startWith: 1000
        );
        #endregion
    }

    public void ss_sequence_ownership()
    {
        var seq = new Sequence("dbo.order_seq");

        #region sample_ss_sequence_ownership
        seq.Owner = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.orders");
        seq.OwnerColumn = "id";
        #endregion
    }

    public void ss_sequence_with_table()
    {
        #region sample_ss_sequence_with_table
        var table = new Table("dbo.orders");
        var seq = new Sequence("dbo.order_seq");

        table.AddColumn<long>("id").AsPrimaryKey()
            .DefaultValueFromSequence(seq);
        #endregion
    }

    public void ss_sequence_ddl()
    {
        var seq = new Sequence("dbo.order_seq");

        #region sample_ss_sequence_ddl
        var migrator = new SqlServerMigrator();
        var writer = new StringWriter();

        seq.WriteCreateStatement(migrator, writer);
        // Output: CREATE SEQUENCE dbo.order_seq START WITH 1;

        seq.WriteDropStatement(migrator, writer);
        // Output: DROP SEQUENCE IF EXISTS dbo.order_seq;
        #endregion
    }

    public async Task ss_sequence_delta_detection()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var seq = new Sequence("dbo.order_seq");

        #region sample_ss_sequence_delta_detection
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        var delta = await seq.FindDeltaAsync(conn);
        // delta.Difference: None or Create
        #endregion
    }

    // table-types.md samples

    public void ss_define_table_type()
    {
        #region sample_ss_define_table_type
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.OrderItemType");
        var tableType = new TableType(identifier);

        tableType.AddColumn<int>("product_id").NotNull();
        tableType.AddColumn<int>("quantity").NotNull();
        tableType.AddColumn("unit_price", "decimal(10,2)");
        #endregion
    }

    public void ss_table_type_columns()
    {
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.OrderItemType");
        var tableType = new TableType(identifier);

        #region sample_ss_table_type_columns
        tableType.AddColumn<string>("name");            // maps to varchar(100)
        tableType.AddColumn("notes", "nvarchar(max)");  // explicit type
        #endregion
    }

    public void ss_table_type_ddl()
    {
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.OrderItemType");
        var tableType = new TableType(identifier);

        #region sample_ss_table_type_ddl
        var migrator = new SqlServerMigrator();
        var writer = new StringWriter();
        tableType.WriteCreateStatement(migrator, writer);
        // Output: CREATE TYPE dbo.OrderItemType AS TABLE (product_id int NOT NULL, ...)
        #endregion
    }

    public async Task ss_table_type_delta_detection()
    {
        var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
        var identifier = DbObjectName.Parse(SqlServerProvider.Instance, "dbo.OrderItemType");
        var tableType = new TableType(identifier);

        #region sample_ss_table_type_delta_detection
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        var delta = await tableType.FindDeltaAsync(conn);
        // delta.Difference: None, Create, or Update
        #endregion
    }
}
