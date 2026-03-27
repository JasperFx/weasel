using Weasel.Core;

namespace Weasel.Postgresql.Tables;

/// <summary>
/// Schema definition for the mt_tenant_assignments table used by sharded multi-tenancy.
/// Maps tenant IDs to their assigned database in the pool.
/// </summary>
public class TenantAssignmentTable : Table
{
    public const string TableName = "mt_tenant_assignments";

    public TenantAssignmentTable(string schemaName)
        : base(new DbObjectName(schemaName, TableName))
    {
        AddColumn<string>("tenant_id").AsPrimaryKey();
        AddColumn<string>("database_id").NotNull();
        AddColumn("assigned_at", "timestamptz").NotNull().DefaultValueByExpression("now()");

        // Foreign key to the database pool table
        ForeignKeys.Add(new ForeignKey("fk_tenant_assignment_database")
        {
            LinkedTable = new DbObjectName(schemaName, DatabasePoolTable.TableName),
            ColumnNames = new[] { "database_id" },
            LinkedNames = new[] { "database_id" }
        });
    }
}
