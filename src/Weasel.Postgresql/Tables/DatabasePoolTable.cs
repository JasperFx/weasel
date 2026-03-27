using Weasel.Core;

namespace Weasel.Postgresql.Tables;

/// <summary>
/// Schema definition for the mt_database_pool table used by sharded multi-tenancy.
/// Tracks the available databases in the pool with their capacity status.
/// </summary>
public class DatabasePoolTable : Table
{
    public const string TableName = "mt_database_pool";

    public DatabasePoolTable(string schemaName)
        : base(new DbObjectName(schemaName, TableName))
    {
        AddColumn<string>("database_id").AsPrimaryKey();
        AddColumn<string>("connection_string").NotNull();
        AddColumn<bool>("is_full").NotNull().DefaultValueByExpression("false");
        AddColumn<int>("tenant_count").NotNull().DefaultValue(0);
    }
}
