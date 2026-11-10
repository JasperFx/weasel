using Weasel.Core.MultiTenancy;

namespace Weasel.SqlServer.MultiTenancy;

/// <summary>
/// SQL Server's <see cref="IMasterTenantTableDialect" /> (weasel#742): the control-table SQL Polecat's
/// <c>MasterTableTenancy</c> has always run against <c>pc_tenants</c>, lifted so a store on
/// <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" /> does not have to carry its own copy.
/// </summary>
/// <remarks>
/// <para>
/// <b>The table shape is the deployed one, not a tidied one.</b> Control tables already exist in
/// production databases, so the columns and constraint names are exactly Polecat's:
/// <c>tenant_id NVARCHAR(200)</c> primary key, <c>connection_string NVARCHAR(MAX) NOT NULL</c> and
/// <c>is_disabled BIT NOT NULL DEFAULT 0</c>, with the constraints named <c>pk_{table}</c> and
/// <c>df_{table}_is_disabled</c> after whatever table name the store passes.
/// </para>
/// <para>
/// <b>Re-adding a tenant re-enables it</b>: the <c>MERGE</c> clears <c>is_disabled</c> on a match.
/// That is Polecat's behaviour and differs from the PostgreSQL dialect, which follows Marten's.
/// </para>
/// <para>
/// <b>Every name is bracketed and every literal escaped, never trusted.</b> The base executes
/// <see cref="CreateControlTable" /> without parameters, so where Polecat bound the schema and the
/// qualified name as <c>@schema</c> / <c>@qualified</c> they are rendered here as escaped
/// <c>N'…'</c> literals, and the schema is created through <c>sp_executesql</c> with
/// <c>QUOTENAME</c> — <c>EXEC('…' + QUOTENAME(…))</c> is a syntax error, because <c>EXEC()</c>
/// accepts only literals and variables.
/// </para>
/// </remarks>
public class SqlServerMasterTenantTableDialect: IMasterTenantTableDialect
{
    /// <summary>Polecat's control-table name.</summary>
    public const string DefaultTableName = "pc_tenants";

    /// <summary>The tenant id column, the table's primary key.</summary>
    public const string TenantIdColumn = "tenant_id";

    /// <summary>The tenant's connection string.</summary>
    public const string ConnectionStringColumn = "connection_string";

    /// <summary>The disabled flag.</summary>
    public const string DisabledColumn = "is_disabled";

    /// <summary>A shared instance; the dialect holds no state.</summary>
    public static readonly SqlServerMasterTenantTableDialect Instance = new();

    /// <summary>
    /// <c>[schema].[table]</c>, always bracketed, with any embedded <c>]</c> doubled.
    /// </summary>
    public virtual string QualifiedTableName(string schemaName, string tableName) =>
        $"{bracket(schemaName)}.{bracket(tableName)}";

    public virtual string CreateControlTable(string schemaName, string tableName, string qualifiedTableName) =>
        $"""
         IF SCHEMA_ID(N'{literal(schemaName)}') IS NULL
         BEGIN
             DECLARE @createSchema NVARCHAR(MAX) = N'CREATE SCHEMA ' + QUOTENAME(N'{literal(schemaName)}');
             EXEC sp_executesql @createSchema;
         END
         IF OBJECT_ID(N'{literal(qualifiedTableName)}', 'U') IS NULL
             CREATE TABLE {qualifiedTableName} (
                 {TenantIdColumn} NVARCHAR(200) NOT NULL CONSTRAINT {bracket("pk_" + tableName)} PRIMARY KEY,
                 {ConnectionStringColumn} NVARCHAR(MAX) NOT NULL,
                 {DisabledColumn} BIT NOT NULL CONSTRAINT {bracket("df_" + tableName + "_" + DisabledColumn)} DEFAULT 0
             );
         """;

    public virtual string SelectEnabledTenants(string qualifiedTableName) =>
        $"SELECT {TenantIdColumn}, {ConnectionStringColumn} FROM {qualifiedTableName} WHERE {DisabledColumn} = 0;";

    public virtual string SelectDisabledTenantIds(string qualifiedTableName) =>
        $"SELECT {TenantIdColumn} FROM {qualifiedTableName} WHERE {DisabledColumn} = 1;";

    public virtual string SelectConnectionString(string qualifiedTableName) =>
        $"SELECT {ConnectionStringColumn} FROM {qualifiedTableName} WHERE {TenantIdColumn} = @id AND {DisabledColumn} = 0;";

    // A MERGE must be terminated by a semicolon; SQL Server refuses it otherwise.
    public virtual string UpsertTenant(string qualifiedTableName) =>
        $"""
         MERGE {qualifiedTableName} AS target
         USING (SELECT @id AS {TenantIdColumn}) AS source
         ON target.{TenantIdColumn} = source.{TenantIdColumn}
         WHEN MATCHED THEN
             UPDATE SET {ConnectionStringColumn} = @connection, {DisabledColumn} = 0
         WHEN NOT MATCHED THEN
             INSERT ({TenantIdColumn}, {ConnectionStringColumn}, {DisabledColumn}) VALUES (@id, @connection, 0);
         """;

    public virtual string DeleteTenant(string qualifiedTableName) =>
        $"DELETE FROM {qualifiedTableName} WHERE {TenantIdColumn} = @id;";

    public virtual string DeleteAllTenants(string qualifiedTableName) =>
        $"DELETE FROM {qualifiedTableName};";

    public virtual string SetTenantDisabled(string qualifiedTableName, bool disabled) =>
        $"UPDATE {qualifiedTableName} SET {DisabledColumn} = {(disabled ? 1 : 0)} WHERE {TenantIdColumn} = @id;";

    private static string bracket(string name) => string.Concat("[", name.Replace("]", "]]"), "]");

    private static string literal(string value) => value.Replace("'", "''");
}
