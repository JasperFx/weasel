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
        return ResolveDatabaseTypeFromMemo(type)
               ?? throw new NotSupportedException(
                   $"Weasel.Oracle does not (yet) support database type mapping to {type.FullNameInCode()}");
    }

    /// <summary>
    ///     The <see cref="OracleDbType" /> for a CLR type, or null when there is no mapping.
    /// </summary>
    /// <remarks>
    ///     This used to end in <c>?? OracleDbType.Varchar2</c>, which could never return null -- so
    ///     <see cref="determineParameterType" /> always reported success and every branch after
    ///     its first was dead code, including weasel#404's "null means unmapped, defer to the
    ///     driver". weasel#758 found the same shape on the generic provider and SQL Server.
    /// </remarks>
    private OracleDbType? ResolveOracleDbType(Type type)
    {
        return ResolveParameterTypeFromMemo(type);
    }

    protected override Type[] determineClrTypesForParameterType(OracleDbType dbType)
    {
        return Type.EmptyTypes;
    }

    /// <inheritdoc />
    public override IdentifierRules Rules => OracleIdentifierRules.Instance;

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

        // Measured against a live server rather than reasoned about: what every provider did here
        // was wrong against the column its own GetDatabaseType generates, and three of the six did
        // not merely convert but threw. An enum now resolves through EnumStorage.AsInteger -- the
        // JasperFx default, and what PostgreSQL, Firebird and SQLite already did in effect -- which
        // also routes it through the enum's *underlying* type, so a long-backed enum is not
        // truncated. An AsString consumer calls ToParameterType(type, EnumStorage.AsString).
        // weasel#761.
        //
        // Oracle's Varchar2 sent the enum's *name*, which worked against an AsString column and
        // threw ORA-01722 against the AsInteger one. As on MySQL, an AsString consumer has to move
        // to the EnumStorage overload.
        if (type.IsEnum)
        {
            dbType = ToParameterType(type, EnumStorage.AsInteger);
            return true;
        }

        // An array is left unmapped, so it defers: TryGetDbType returns null and the parameter goes
        // out untyped for the driver to infer. The refusal the dead branch carried was never
        // reachable, and overriding a driver that can handle an array -- a table-valued parameter,
        // MySqlConnector's own handling -- would be the worse of the two answers. byte[] never
        // reaches here; it is in the memo. So there is deliberately no `type.IsArray` branch
        // below. weasel#761.

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
        // Oracle cannot handle raw Guid values - convert to byte[] for RAW(16) storage
        if (parameter.Value is Guid guidValue)
        {
            parameter.Value = guidValue.ToByteArray();
            parameter.OracleDbType = OracleDbType.Raw;
        }

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
