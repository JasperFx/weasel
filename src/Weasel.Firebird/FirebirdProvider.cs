using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Types;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Firebird;

/// <summary>
///     Firebird's .NET-to-database type mappings, for Firebird 3, 4 and 5.
/// </summary>
/// <remarks>
///     <list type="table">
///         <listheader><term>.NET</term><description>Firebird</description></listheader>
///         <item><term>string</term><description><c>VARCHAR(255)</c></description></item>
///         <item><term>bool</term><description><c>BOOLEAN</c></description></item>
///         <item><term>byte, short</term><description><c>SMALLINT</c></description></item>
///         <item><term>int</term><description><c>INTEGER</c></description></item>
///         <item><term>long</term><description><c>BIGINT</c></description></item>
///         <item><term>decimal</term><description><c>NUMERIC(18,4)</c></description></item>
///         <item><term>double</term><description><c>DOUBLE PRECISION</c></description></item>
///         <item><term>float</term><description><c>FLOAT</c></description></item>
///         <item><term>DateTime</term><description><c>TIMESTAMP</c></description></item>
///         <item><term>DateOnly</term><description><c>DATE</c></description></item>
///         <item><term>TimeOnly, TimeSpan</term><description><c>TIME</c></description></item>
///         <item><term>DateTimeOffset</term><description><c>TIMESTAMP WITH TIME ZONE</c> (Firebird 4+)</description></item>
///         <item><term>Guid</term><description><c>CHAR(16) CHARACTER SET OCTETS</c></description></item>
///         <item><term>byte[]</term><description><c>BLOB SUB_TYPE BINARY</c></description></item>
///         <item><term>anything else</term><description><c>BLOB SUB_TYPE TEXT</c>, as JSON</description></item>
///     </list>
/// </remarks>
public class FirebirdProvider: DatabaseProvider<FbCommand, FbParameter, FbDbType>
{
    public const string EngineName = "Firebird";
    public static readonly FirebirdProvider Instance = new();

    private FirebirdProvider(): base(FirebirdObjectName.DefaultSchema)
    {
    }

    protected override void storeMappings()
    {
        store<string>(FbDbType.VarChar, "VARCHAR(255)");
        store<bool>(FbDbType.Boolean, "BOOLEAN");
        store<byte>(FbDbType.SmallInt, "SMALLINT");
        store<short>(FbDbType.SmallInt, "SMALLINT");
        store<int>(FbDbType.Integer, "INTEGER");
        store<long>(FbDbType.BigInt, "BIGINT");
        store<decimal>(FbDbType.Numeric, "NUMERIC(18,4)");
        store<double>(FbDbType.Double, "DOUBLE PRECISION");
        store<float>(FbDbType.Float, "FLOAT");
        store<DateTime>(FbDbType.TimeStamp, "TIMESTAMP");
        store<DateOnly>(FbDbType.Date, "DATE");
        store<TimeOnly>(FbDbType.Time, "TIME");
        store<TimeSpan>(FbDbType.Time, "TIME");
        store<DateTimeOffset>(FbDbType.TimeStampTZ, "TIMESTAMP WITH TIME ZONE");
        store<Guid>(FbDbType.Guid, "CHAR(16) CHARACTER SET OCTETS");
        store<byte[]>(FbDbType.Binary, "BLOB SUB_TYPE BINARY");
    }

    /// <summary>
    ///     The column type a document stored as JSON gets.
    /// </summary>
    public const string JsonColumnType = "BLOB SUB_TYPE TEXT";

    protected override Type[] determineClrTypesForParameterType(FbDbType dbType)
    {
        return Type.EmptyTypes;
    }

    /// <inheritdoc />
    public override IdentifierRules Rules => FirebirdIdentifierRules.Instance;

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new FirebirdObjectName(schemaName, objectName);

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        var builder = new FbConnectionStringBuilder(connectionString) { ApplicationName = applicationName };
        return builder.ConnectionString;
    }

    protected override bool determineParameterType(Type type, out FbDbType dbType)
    {
        if (ResolveParameterTypeFromMemo(type) is { } stored)
        {
            dbType = stored;
            return true;
        }

        if (type.IsNullable())
        {
            dbType = ToParameterType(type.GetInnerTypeFromNullable());
            return true;
        }

        if (type.IsEnum)
        {
            dbType = FbDbType.Integer;
            return true;
        }

        if (type.IsArray)
        {
            throw new NotSupportedException("Firebird does not support arrays as parameters");
        }

        if (type == typeof(DBNull))
        {
            dbType = FbDbType.VarChar;
            return true;
        }

        if (type.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
            return true;
        }

        dbType = FbDbType.VarChar;
        return false;
    }

    public override string GetDatabaseType(Type memberType, EnumStorage enumStyle)
    {
        if (memberType.IsEnum)
        {
            return enumStyle == EnumStorage.AsInteger ? "INTEGER" : "VARCHAR(100)";
        }

        if (memberType.IsNullable())
        {
            return GetDatabaseType(memberType.GetInnerTypeFromNullable(), enumStyle);
        }

        if (ResolveDatabaseTypeFromMemo(memberType) is { } result)
        {
            return result;
        }

        // byte[] is in the memo; any other array has no Firebird column type.
        if (memberType.IsArray)
        {
            throw new NotSupportedException("Firebird does not support array column types");
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        return JsonColumnType;
    }

    public override void AddParameter(FbCommand command, FbParameter parameter)
    {
        parameter.Value = NormalizeValue(parameter.Value);
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(FbParameter parameter, FbDbType dbType)
    {
        parameter.FbDbType = dbType;
    }

    /// <summary>
    ///     FirebirdClient rejects a <see cref="DateTimeOffset" /> as a parameter value; a
    ///     <c>TIMESTAMP WITH TIME ZONE</c> travels as <see cref="FbZonedDateTime" />. The instant is
    ///     kept, in UTC.
    /// </summary>
    internal static object? NormalizeValue(object? value)
        => value is DateTimeOffset offset ? new FbZonedDateTime(offset.UtcDateTime, "UTC") : value;

    /// <summary>
    ///     A foreign key rule as <c>RDB$REF_CONSTRAINTS</c> spells it: <c>RESTRICT</c> (no clause was
    ///     written), <c>NO ACTION</c>, <c>CASCADE</c>, <c>SET NULL</c> or <c>SET DEFAULT</c>.
    /// </summary>
    public static CascadeAction ReadAction(string? description)
    {
        switch (description?.ToUpperInvariant().Trim())
        {
            case "CASCADE":
                return CascadeAction.Cascade;
            case "SET NULL":
                return CascadeAction.SetNull;
            case "SET DEFAULT":
                return CascadeAction.SetDefault;
            case "RESTRICT":
                return CascadeAction.Restrict;
        }

        return CascadeAction.NoAction;
    }
}
