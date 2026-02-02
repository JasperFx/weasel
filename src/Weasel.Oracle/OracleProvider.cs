using System.Data;
using ImTools;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle;

public class OracleProvider: DatabaseProvider<OracleCommand, OracleParameter, OracleDbType>
{
    public const string EngineName = "Oracle";
    public static readonly OracleProvider Instance = new();

    private OracleProvider(): base("WEASEL")
    {
    }

    protected override void storeMappings()
    {
        store<string>(OracleDbType.Varchar2, "VARCHAR2(4000)");
        store<bool>(OracleDbType.Int16, "NUMBER(1)");
        store<byte>(OracleDbType.Byte, "NUMBER(3)");
        store<short>(OracleDbType.Int16, "NUMBER(5)");
        store<int>(OracleDbType.Int32, "NUMBER(10)");
        store<long>(OracleDbType.Int64, "NUMBER(19)");
        store<byte[]>(OracleDbType.Blob, "BLOB");
        store<DateTime>(OracleDbType.Date, "DATE");
        store<DateTimeOffset>(OracleDbType.TimeStampTZ, "TIMESTAMP WITH TIME ZONE");
        store<decimal>(OracleDbType.Decimal, "NUMBER");
        store<double>(OracleDbType.Double, "BINARY_DOUBLE");
        store<float>(OracleDbType.Single, "BINARY_FLOAT");
        store<TimeSpan>(OracleDbType.IntervalDS, "INTERVAL DAY TO SECOND");
        store<Guid>(OracleDbType.Raw, "RAW(16)");
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
                $"Weasel.Oracle does not (yet) support database type mapping to {type.FullNameInCode()}");

        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
        return databaseType;
    }

    private OracleDbType? ResolveOracleDbType(Type type)
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

        return OracleDbType.Varchar2;
    }

    protected override Type[] determineClrTypesForParameterType(OracleDbType dbType)
    {
        return Type.EmptyTypes;
    }

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new OracleObjectName(schemaName, objectName);

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        var builder = new OracleConnectionStringBuilder(connectionString);
        return builder.ConnectionString;
    }

    public string ConvertSynonyms(string type)
    {
        var upper = type.ToUpperInvariant();

        switch (upper)
        {
            case "INT":
            case "INTEGER":
                return "NUMBER(10)";
            case "SMALLINT":
                return "NUMBER(5)";
            case "BIGINT":
                return "NUMBER(19)";
            case "BOOLEAN":
            case "BOOL":
                return "NUMBER(1)";
            case "TEXT":
            case "STRING":
                return "VARCHAR2(4000)";
            case "FLOAT":
                return "BINARY_FLOAT";
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return "BINARY_DOUBLE";
        }

        // Handle TIMESTAMP variants - normalize by stripping precision like (6)
        if (upper.StartsWith("TIMESTAMP"))
        {
            var normalized = System.Text.RegularExpressions.Regex.Replace(upper, @"\(\d+\)", "");
            return normalized.Replace("  ", " ");
        }

        return upper;
    }

    protected override bool determineParameterType(Type type, out OracleDbType dbType)
    {
        var oracleDbType = ResolveOracleDbType(type);
        if (oracleDbType != null)
        {
            dbType = oracleDbType.Value;
            return true;
        }

        if (type.IsNullable())
        {
            dbType = ToParameterType(type.GetInnerTypeFromNullable());
            return true;
        }

        if (type.IsEnum)
        {
            dbType = OracleDbType.Int32;
            return true;
        }

        if (type.IsArray)
        {
            throw new NotSupportedException("Oracle does not support arrays as parameters");
        }

        if (type == typeof(DBNull))
        {
            dbType = OracleDbType.Varchar2;
            return true;
        }

        if (type.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
            return true;
        }

        dbType = OracleDbType.Varchar2;
        return false;
    }

    public override string GetDatabaseType(Type memberType, EnumStorage enumStyle)
    {
        if (memberType.IsEnum)
        {
            return enumStyle == EnumStorage.AsInteger ? "NUMBER(10)" : "VARCHAR2(100)";
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
            throw new NotSupportedException("Oracle does not support array column types");
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        return "CLOB";
    }

    public override void AddParameter(OracleCommand command, OracleParameter parameter)
    {
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(OracleParameter parameter, OracleDbType dbType)
    {
        parameter.OracleDbType = dbType;
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
        }

        return CascadeAction.NoAction;
    }
}
