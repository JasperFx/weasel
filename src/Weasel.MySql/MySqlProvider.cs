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
        if (DatabaseTypeMemo.Value.TryFind(type, out var value))
        {
            return value;
        }

        if (!type.IsNullable() ||
            !DatabaseTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out var databaseType))
            throw new NotSupportedException(
                $"Weasel.MySql does not (yet) support database type mapping to {type.FullNameInCode()}");

        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
        return databaseType;
    }

    private MySqlDbType? ResolveMySqlDbType(Type type)
    {
        if (ParameterTypeMemo.Value.TryFind(type, out var value))
        {
            return value;
        }

        if (type.IsNullable() &&
            ParameterTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out var parameterType))
        {
            ParameterTypeMemo.Swap(d => d.AddOrUpdate(type, parameterType));
            return parameterType;
        }

        return MySqlDbType.VarChar;
    }

    protected override Type[] determineClrTypesForParameterType(MySqlDbType dbType)
    {
        return Type.EmptyTypes;
    }

    public override string ToQualifiedName(string objectName) =>
        string.IsNullOrEmpty(objectName) ? objectName : $"`{objectName}`";

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

        if (type.IsEnum)
        {
            dbType = MySqlDbType.Int32;
            return true;
        }

        if (type.IsArray)
        {
            throw new NotSupportedException("MySql does not support arrays as parameters");
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
