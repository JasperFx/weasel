using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using ImTools;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql;

public class PostgresqlProvider: DatabaseProvider<NpgsqlCommand, NpgsqlParameter, NpgsqlDbType>
{
    public const string EngineName = "PostgreSQL";

    public static readonly PostgresqlProvider Instance = new();

    private PostgresqlProvider(): base("public")
    {
    }

    public List<Type> ContainmentOperatorTypes { get; } = new();
    public List<Type> TimespanTypes { get; } = new();
    public List<Type> TimespanZTypes { get; } = new();

    [Obsolete("This property no longer has any effect. Identifiers are now always quoted when they are keywords or contain uppercase characters. This property will be removed in a future version.")]
    public bool UseCaseSensitiveQualifiedNames { get; set; } = false;

    protected override void storeMappings()
    {
        // Initialize PgTypeMemo with Types which are not available in Npgsql mappings
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(long), "bigint"));
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(Guid), "uuid"));
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(string), "varchar"));
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(float), "real"));

        // Default Npgsql mapping is 'numeric' but we are using 'decimal'
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(decimal), "decimal"));

        // Default Npgsql mappings is 'timestamp' but we are using 'timestamp without time zone'
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(DateTime), "timestamp without time zone"));

        AddTimespanTypes(NpgsqlDbType.Timestamp, ResolveTypes(NpgsqlDbType.Timestamp));
        AddTimespanTypes(NpgsqlDbType.TimestampTz, ResolveTypes(NpgsqlDbType.TimestampTz));

        RegisterMapping(typeof(uint), "oid", NpgsqlDbType.Oid);
    }


    // Lazily retrieve the CLR type to NpgsqlDbType and PgTypeName mapping from exposed INpgsqlTypeMapper.Mappings.
    // This is lazily calculated instead of precached because it allows consuming code to register
    // custom npgsql mappings prior to execution.
    private string? ResolveDatabaseType(Type type)
    {
        // Try the shared base memo (with nullable-promote) first; on a miss, ask
        // Npgsql's type-mapping plugin (handles JsonB, NpgsqlRange<T>, …) and
        // cache the answer so subsequent lookups are O(1).
        var cached = ResolveDatabaseTypeFromMemo(type);
        if (cached != null)
        {
            return cached;
        }

        var value = GetTypeMapping(type)?.DataTypeName;
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, value));
        return value;
    }

    private NpgsqlDbType? ResolveNpgsqlDbType(Type type)
    {
        // Same pattern as ResolveDatabaseType but for the NpgsqlDbType enum.
        var cached = ResolveParameterTypeFromMemo(type);
        if (cached != null)
        {
            return cached;
        }

        var value = GetTypeMapping(type)?.NpgsqlDbType;
        ParameterTypeMemo.Swap(d => d.AddOrUpdate(type, value));
        return value;
    }

    protected override Type[] determineClrTypesForParameterType(NpgsqlDbType dbType)
    {
        return GetTypeMapping(dbType)?.ClrTypes ?? Type.EmptyTypes;
    }

    private static NpgsqlTypeMapping? GetTypeMapping(Type type)
    {
        return NpgsqlTypeMapper
            .Mappings
            .LastOrDefault(mapping => mapping.ClrTypes.Contains(type));
    }

    private static NpgsqlTypeMapping? GetTypeMapping(NpgsqlDbType type)
    {
        return NpgsqlTypeMapper
            .Mappings
            .LastOrDefault(mapping => mapping.NpgsqlDbType == type);
    }

    public string ConvertSynonyms(string type)
    {
        switch (type.ToLowerInvariant())
        {
            case "character varying":
            case "varchar":
                return "varchar";

            case "boolean":
            case "bool":
                return "boolean";

            case "integer":
            case "serial":
                return "int";

            case "bigserial":
                return "bigint";

            case "smallserial":
                return "smallint";

            case "integer[]":
                return "int[]";

            case "boolean[]":
            case "bool[]":
                return "boolean[]";

            case "decimal[]":
            case "numeric[]":
                return "decimal[]";

            case "uuid[]":
                return "uuid[]";

            case "smallint[]":
                return "smallint[]";

            case "bigint[]":
                return "bigint[]";

            case "real[]":
                return "real[]";

            case "double precision[]":
                return "double precision[]";

            case "timestamp without time zone[]":
                return "timestamp[]";

            case "timestamp with time zone[]":
                return "timestamptz[]";

            case "decimal":
            case "numeric":
                return "decimal";

            case "timestamp without time zone":
                return "timestamp";

            case "timestamp with time zone":
                return "timestamptz";

            case "array":
            case "character varying[]":
            case "varchar[]":
            case "text[]":
                return "array";

            case "_text":
                return "array";
        }

        return type;
    }

    // Reflective interface enumeration on `type.ImplementedInterfaces` is gated by
    // earlier memo / IsNullable / IsArray / IsEnum branches — only types Npgsql doesn't
    // ship a built-in mapping for and that aren't directly enumerable hit this path.
    // The base contract for determineParameterType in DatabaseProvider<,,> is a
    // cold-path resolver, so AOT consumers that target only the pre-mapped types
    // never traverse this. weasel#263 audit.
    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Reflective IEnumerable<T> detection on an arbitrary CLR type. Only reached for types not in the memo / not arrays / not enums; AOT consumers that pre-register their types via storeMappings never hit this path. weasel#263.")]
    protected override bool determineParameterType(Type type, out NpgsqlDbType dbType)
    {
        var npgsqlDbType = ResolveNpgsqlDbType(type);
        if (npgsqlDbType != null)
        {
            dbType = npgsqlDbType.Value;
            return true;
        }

        if (type.IsNullable())
        {
            dbType = ToParameterType(type.GetInnerTypeFromNullable());
            return true;
        }

        if (type.IsEnum)
        {
            dbType = NpgsqlDbType.Integer;
            return true;
        }

        if (type.IsArray)
        {
            if (type == typeof(byte[]))
            {
                dbType = NpgsqlDbType.Bytea;
                return true;
            }

            dbType = NpgsqlDbType.Array | ToParameterType(type.GetElementType()!);
            return true;
        }

        var typeInfo = type.GetTypeInfo();

        var ilist = typeInfo.ImplementedInterfaces.FirstOrDefault(x =>
            x.GetTypeInfo().IsGenericType && x.IsGenericEnumerable()
        );

        if (ilist != null)
        {
            dbType = NpgsqlDbType.Array | ToParameterType(ilist.GetGenericArguments()[0]);
            return true;
        }

        if (typeInfo.IsGenericType && type.GetGenericTypeDefinition() == typeof(NpgsqlRange<>))
        {
            dbType = NpgsqlDbType.Range | ToParameterType(type.GetGenericArguments()[0]);
            return true;
        }

        if (type == typeof(DBNull))
        {
            dbType = NpgsqlDbType.Unknown;
            return true;
        }

        if (typeInfo.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
            return true;
        }

        dbType = NpgsqlDbType.Unknown;
        return false;
    }

    public override string GetDatabaseType(Type memberType, EnumStorage enumStyle)
    {
        if (memberType.IsEnum)
        {
            return enumStyle == EnumStorage.AsInteger ? "integer" : "varchar";
        }

        if (memberType.IsArray)
        {
            return GetDatabaseType(memberType.GetElementType()!, enumStyle) + "[]";
        }

        if (memberType.IsNullable())
        {
            return GetDatabaseType(memberType.GetInnerTypeFromNullable(), enumStyle);
        }

        if (ResolveDatabaseType(memberType) is { } result)
        {
            return result;
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        return "jsonb";
    }

    public override void AddParameter(NpgsqlCommand command, NpgsqlParameter parameter)
    {
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(NpgsqlParameter parameter, NpgsqlDbType dbType)
    {
        parameter.NpgsqlDbType = dbType;
    }

    public bool HasTypeMapping(Type memberType)
    {
        if (memberType.IsNullable())
        {
            return HasTypeMapping(memberType.GetInnerTypeFromNullable());
        }

        // more complicated later
        return ResolveDatabaseType(memberType) != null || memberType.IsEnum;
    }

    /// <summary>
    ///     Construct a closed <see cref="Nullable{T}" /> over the supplied value type.
    ///     Uses <see cref="Type.MakeGenericType" /> at runtime, which AOT can't generate
    ///     code for ahead of time — AOT consumers that pre-register their types via
    ///     <see cref="storeMappings" /> bypass this helper and never JIT a new closed
    ///     <c>Nullable&lt;T&gt;</c>. weasel#263 audit.
    /// </summary>
    [RequiresDynamicCode("Type.MakeGenericType(Nullable<>) requires runtime IL generation. Pre-register types via storeMappings to avoid this path when publishing AOT.")]
    private Type GetNullableType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type.IsValueType)
        {
            return typeof(Nullable<>).MakeGenericType(type);
        }

        return type;
    }

    /// <summary>
    ///     Register additional CLR types to be treated as PostgreSQL <c>timestamp</c> /
    ///     <c>timestamptz</c>. Internally calls <see cref="GetNullableType" /> which
    ///     constructs closed <see cref="Nullable{T}" /> generics via
    ///     <see cref="Type.MakeGenericType" />. <see cref="storeMappings" /> overrides an
    ///     abstract base and can't be annotated with <see cref="RequiresDynamicCodeAttribute" />
    ///     without IL3051 against <c>DatabaseProvider&lt;,,&gt;.storeMappings</c>, so we
    ///     <see cref="UnconditionalSuppressMessageAttribute">suppress</see> the IL3050 here
    ///     with a Justification. AOT consumers that pre-register the nullable form alongside
    ///     the value type via <see cref="RegisterMapping" /> bypass this helper entirely.
    ///     weasel#263.
    /// </summary>
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Calls GetNullableType which uses Type.MakeGenericType for closed Nullable<T> construction. Pre-registering both T and Nullable<T> via RegisterMapping avoids this path; documented in the AOT audit (weasel#263).")]
    public void AddTimespanTypes(NpgsqlDbType npgsqlDbType, params Type[] types)
    {
        var timespanTypesList = npgsqlDbType == NpgsqlDbType.Timestamp ? TimespanTypes : TimespanZTypes;
        var typesWithNullables = types.Union(types.Select(GetNullableType))
            .Where(t => !timespanTypesList.Contains(t)).ToList();

        timespanTypesList.AddRange(typesWithNullables);

        ContainmentOperatorTypes.AddRange(typesWithNullables);
    }

    public override string ToQualifiedName(string objectName)
    {
        if (string.IsNullOrEmpty(objectName) || (objectName.Length >= 2 && objectName[0] == '"' && objectName[^1] == '"'))
            return objectName;

        return SchemaUtils.QuoteName(objectName);
    }

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new PostgresqlObjectName(schemaName, objectName, ToQualifiedName(schemaName, objectName));

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        builder.ApplicationName = applicationName;
        return builder.ConnectionString;
    }
}
