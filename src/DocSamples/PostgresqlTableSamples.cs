using Npgsql;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;

namespace DocSamples;

public class PostgresqlTableSamples
{
    public void create_a_table()
    {
        #region sample_pg_create_a_table
        // Create a table in the default "public" schema
        var table = new Table("users");

        // Create a table in a specific schema
        var schemaTable = new Table("myschema.users");
        #endregion
    }

    public void add_columns()
    {
        #region sample_pg_add_columns
        var table = new Table("users");

        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull();
        table.AddColumn<DateTime>("created_at").NotNull();
        table.AddColumn("metadata", "jsonb");
        #endregion
    }

    public void primary_keys()
    {
        #region sample_pg_primary_keys
        var table = new Table("orders");

        // Single column
        table.AddColumn<Guid>("id").AsPrimaryKey();

        // Composite key
        var compositeTable = new Table("tenant_orders");
        compositeTable.AddColumn<int>("tenant_id").AsPrimaryKey();
        compositeTable.AddColumn<int>("order_id").AsPrimaryKey();
        #endregion
    }

    public void foreign_keys()
    {
        #region sample_pg_foreign_keys
        var table = new Table("employees");

        table.AddColumn<int>("company_id")
            .ForeignKeyTo("companies", "id",
                onDelete: CascadeAction.Cascade);
        #endregion
    }

    public void indexes()
    {
        #region sample_pg_indexes
        var table = new Table("users");

        // Simple unique index
        var index = new IndexDefinition("idx_users_email")
        {
            IsUnique = true,
            Method = IndexMethod.btree
        };
        index.Columns = new[] { "email" };
        table.Indexes.Add(index);
        #endregion
    }

    public void default_values()
    {
        #region sample_pg_default_values
        var table = new Table("tasks");

        table.AddColumn<bool>("is_active").DefaultValueByExpression("true");
        table.AddColumn<int>("priority").DefaultValue(0);
        table.AddColumn<string>("status").DefaultValueByString("pending");
        table.AddColumn<DateTimeOffset>("created_at")
            .DefaultValueByExpression("now()");
        #endregion
    }

    public async Task delta_detection()
    {
        #region sample_pg_table_delta_detection
        var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
        var table = new Table("users");

        await using var conn = dataSource.CreateConnection();
        await conn.OpenAsync();

        // Check if table exists
        bool exists = await table.ExistsInDatabaseAsync(conn);

        // Fetch the existing table definition from the database
        var existing = await table.FetchExistingAsync(conn);

        // Compare and generate migration DDL
        var delta = new TableDelta(table, existing);
        // delta.Difference tells you: None, Create, Update, or Recreate
        #endregion
    }

    public void generate_ddl()
    {
        #region sample_pg_table_generate_ddl
        var table = new Table("users");

        var migrator = new PostgresqlMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        Console.WriteLine(writer.ToString());
        #endregion
    }
}
