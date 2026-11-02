using System.Data;
using ImTools;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql;

public class MySqlProvider: DatabaseProvider<MySqlCommand, MySqlParameter, MySqlDbType>
{
    public const string EngineName = "MySql";
    public static readonly MySqlProvider Instance = new();

    private MySqlProvider(): base("public")
    {
    }

    protected override void storeMappings()
    {
        store<string>(MySqlDbType.VarChar, "VARCHAR(255)");
        store<bool>(MySqlDbType.Bool, "TINYINT(1)");
        store<byte>(MySqlDbType.UByte, "TINYINT UNSIGNED");
        store<short>(MySqlDbType.Int16, "SMALLINT");
        store<int>(MySqlDbType.Int32, "INT");
        store<long>(MySqlDbType.Int64, "BIGINT");
        store<byte[]>(MySqlDbType.Blob, "BLOB");
        store<DateTime>(MySqlDbType.DateTime, "DATETIME");
        store<DateTimeOffset>(MySqlDbType.DateTime, "DATETIME");
        store<decimal>(MySqlDbType.Decimal, "DECIMAL(18,2)");
        store<double>(MySqlDbType.Double, "DOUBLE");
        store<float>(MySqlDbType.Float, "FLOAT");
        store<TimeSpan>(MySqlDbType.Time, "TIME");
        store<Guid>(MySqlDbType.Guid, "CHAR(36)");
    }

    private string? ResolveDatabaseType(Type type)
    {
        return ResolveDatabaseTypeFromMemo(type)
               ?? throw new NotSupportedException(
                   $"Weasel.MySql does not (yet) support database type mapping to {type.FullNameInCode()}");
    }

    /// <summary>
    ///     The <see cref="MySqlDbType" /> for a CLR type, or null when there is no mapping.
    /// </summary>
    /// <remarks>
    ///     This used to end in <c>?? MySqlDbType.VarChar</c>, which could never return null -- so
    ///     <see cref="determineParameterType" /> always reported success and every branch after
    ///     its first was dead code, including weasel#404's "null means unmapped, defer to the
    ///     driver". weasel#758 found the same shape on the generic provider and SQL Server.
    /// </remarks>
    private MySqlDbType? ResolveMySqlDbType(Type type)
    {
        return ResolveParameterTypeFromMemo(type);
    }

    protected override Type[] determineClrTypesForParameterType(MySqlDbType dbType)
    {
        return Type.EmptyTypes;
    }

    public override string ToQualifiedName(string objectName) =>
        string.IsNullOrEmpty(objectName) ? objectName : $"{SchemaUtils.QuoteName(objectName)}";

    /// <inheritdoc />
    public override IdentifierRules Rules => MySqlIdentifierRules.Instance;

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new MySqlObjectName(schemaName, objectName);

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString);
        builder.ApplicationName = applicationName;
        return builder.ConnectionString;
    }

    public string ConvertSynonyms(string type)
    {
        switch (type.ToUpperInvariant())
        {
            case "INTEGER":
                return "INT";
            case "BOOLEAN":
            case "BOOL":
                return "TINYINT(1)";
            case "TEXT":
            case "STRING":
                return "VARCHAR(255)";
            case "REAL":
                return "FLOAT";
        }

        return type.ToUpperInvariant();
    }

    protected override bool determineParameterType(Type type, out MySqlDbType dbType)
    {
        var mySqlDbType = ResolveMySqlDbType(type);
        if (mySqlDbType != null)
        {
            dbType = mySqlDbType.Value;
            return true;
        }

        if (type.IsNullable())
        {
            dbType = ToParameterType(type.GetInnerTypeFromNullable());
            return true;
        }

        // Enums and arrays keep the VarChar catch-all they have always resolved to. The Int32
        // this branch used to carry, and the array refusal that followed it, were both
        // unreachable; turning either on is its own behaviour change, tracked separately.
        if (type.IsEnum || type.IsArray)
        {
            dbType = MySqlDbType.VarChar;
            return true;
        }

        if (type == typeof(DBNull))
        {
            dbType = MySqlDbType.VarChar;
            return true;
        }

        if (type.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
            return true;
        }

        dbType = MySqlDbType.VarChar;
        return false;
    }

    public override string GetDatabaseType(Type memberType, EnumStorage enumStyle)
    {
        if (memberType.IsEnum)
        {
            return enumStyle == EnumStorage.AsInteger ? "INT" : "VARCHAR(100)";
        }

        if (memberType.IsNullable())
        {
            return GetDatabaseType(memberType.GetInnerTypeFromNullable(), enumStyle);
        }

        // Check memo first - this handles byte[] -> BLOB mapping
        if (ResolveDatabaseType(memberType) is { } result)
        {
            return result;
        }

        // byte[] is already handled above by ResolveDatabaseType
        if (memberType.IsArray)
        {
            throw new NotSupportedException("MySql does not support array column types");
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        return "TEXT";
    }

    public override void AddParameter(MySqlCommand command, MySqlParameter parameter)
    {
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(MySqlParameter parameter, MySqlDbType dbType)
    {
        parameter.MySqlDbType = dbType;
    }

    public static CascadeAction ReadAction(string description)
    {
        switch (description.ToUpperInvariant().Trim())
        {
            case "CASCADE":
                return CascadeAction.Cascade;
            case "NO ACTION":
            case "NO_ACTION":
                return CascadeAction.NoAction;
            case "SET NULL":
            case "SET_NULL":
                return CascadeAction.SetNull;
            case "SET DEFAULT":
            case "SET_DEFAULT":
                return CascadeAction.SetDefault;
            case "RESTRICT":
                return CascadeAction.Restrict;
        }

        return CascadeAction.NoAction;
    }
}
