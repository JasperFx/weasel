using Weasel.Core;
using Weasel.Core.MultiTenancy;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql.MultiTenancy;

/// <summary>
/// PostgreSQL's <see cref="IMasterTenantTableDialect" /> (weasel#742): the control-table SQL Marten's
/// <c>MasterTableTenancy</c> has always run against <c>mt_tenant_databases</c>, lifted so a store on
/// <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" /> does not have to carry its own copy.
/// </summary>
/// <remarks>
/// <para>
/// <b>The table shape is the deployed one, not a tidied one.</b> Control tables already exist in
/// production databases and operators populate them by hand, so the columns are exactly Marten's:
/// <c>tenant_id varchar</c> primary key, <c>connection_string varchar not null</c>, and the flag
/// named <c>disabled</c> (not <c>is_disabled</c>), <c>boolean not null default false</c>.
/// </para>
/// <para>
/// <b>The DDL is rendered from <see cref="BuildControlTable" />, not written by hand</b>, so it is
/// the same table Weasel's migration pipeline would create for that definition — constraint name
/// included. A store that provisions through its migration pipeline instead (as Marten does, by
/// overriding <c>ProvisionControlTableAsync</c>) should register that same table, and the two
/// cannot drift into a permanent delta.
/// </para>
/// <para>
/// <b>Re-adding a tenant does not re-enable it</b>: the upsert updates only the connection string.
/// That is Marten's behaviour and differs from SQL Server's dialect, which follows Polecat's and
/// re-enables; <see cref="IMasterTenantTableDialect.UpsertTenant" /> leaves the choice to the dialect.
/// </para>
/// </remarks>
public class PostgresqlMasterTenantTableDialect: IMasterTenantTableDialect
{
    /// <summary>Marten's control-table name.</summary>
    public const string DefaultTableName = "mt_tenant_databases";

    /// <summary>The tenant id column, the table's primary key.</summary>
    public const string TenantIdColumn = "tenant_id";

    /// <summary>The tenant's connection string.</summary>
    public const string ConnectionStringColumn = "connection_string";

    /// <summary>The disabled flag.</summary>
    public const string DisabledColumn = "disabled";

    /// <summary>A shared instance; the dialect holds no state.</summary>
    public static readonly PostgresqlMasterTenantTableDialect Instance = new();

    /// <summary>
    /// The control table as a Weasel table definition — what <see cref="CreateControlTable" />
    /// renders, and what a store provisioning through its own migration pipeline should register.
    /// </summary>
    public static Table BuildControlTable(string schemaName, string tableName)
    {
        var table = new Table(new PostgresqlObjectName(schemaName, tableName, SchemaUtils.IdentifierUsage.General));
        table.AddColumn<string>(TenantIdColumn).AsPrimaryKey();
        table.AddColumn<string>(ConnectionStringColumn).NotNull();
        table.AddColumn<bool>(DisabledColumn).DefaultValueByExpression("false").NotNull();
        return table;
    }

    public virtual string QualifiedTableName(string schemaName, string tableName) =>
        new PostgresqlObjectName(schemaName, tableName, SchemaUtils.IdentifierUsage.General).ToString();

    public virtual string CreateControlTable(string schemaName, string tableName, string qualifiedTableName)
    {
        var migrator = new PostgresqlMigrator
        {
            Formatting = SqlFormatting.Concise,
            TableCreation = CreationStyle.CreateIfNotExists
        };

        var writer = new StringWriter();
        writer.WriteLine(PostgresqlMigrator.CreateSchemaStatementFor(schemaName));
        BuildControlTable(schemaName, tableName).WriteCreateStatement(migrator, writer);
        return writer.ToString();
    }

    public virtual string SelectEnabledTenants(string qualifiedTableName) =>
        $"select {TenantIdColumn}, {ConnectionStringColumn} from {qualifiedTableName} where {DisabledColumn} = false";

    public virtual string SelectDisabledTenantIds(string qualifiedTableName) =>
        $"select {TenantIdColumn} from {qualifiedTableName} where {DisabledColumn} = true";

    public virtual string SelectConnectionString(string qualifiedTableName) =>
        $"select {ConnectionStringColumn} from {qualifiedTableName} where {TenantIdColumn} = @id and {DisabledColumn} = false";

    public virtual string UpsertTenant(string qualifiedTableName) =>
        $"insert into {qualifiedTableName} ({TenantIdColumn}, {ConnectionStringColumn}) values (@id, @connection) " +
        $"on conflict ({TenantIdColumn}) do update set {ConnectionStringColumn} = excluded.{ConnectionStringColumn}";

    public virtual string DeleteTenant(string qualifiedTableName) =>
        $"delete from {qualifiedTableName} where {TenantIdColumn} = @id";

    public virtual string DeleteAllTenants(string qualifiedTableName) =>
        $"delete from {qualifiedTableName}";

    public virtual string SetTenantDisabled(string qualifiedTableName, bool disabled) =>
        $"update {qualifiedTableName} set {DisabledColumn} = {(disabled ? "true" : "false")} where {TenantIdColumn} = @id";
}
