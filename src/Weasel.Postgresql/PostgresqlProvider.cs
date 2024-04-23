using System.Reflection;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql;

public class PostgresqlProvider: DatabaseProvider<NpgsqlCommand, NpgsqlParameter, NpgsqlDbType>
{
    public static readonly PostgresqlProvider Instance = new();

    private PostgresqlProvider(): base("public")
    {
    }

    public List<Type> ContainmentOperatorTypes { get; } = new();
    public List<Type> TimespanTypes { get; } = new();
    public List<Type> TimespanZTypes { get; } = new();

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
        if (DatabaseTypeMemo.Value.TryFind(type, out var value))
        {
            return value;
        }

        value = GetTypeMapping(type)?.DataTypeName;

        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, value));

        return value;
    }

    private NpgsqlDbType? ResolveNpgsqlDbType(Type type)
    {
        if (ParameterTypeMemo.Value.TryFind(type, out var value))
        {
            return value;
        }

        value = GetTypeMapping(type)?.NpgsqlDbType;

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
        switch (type.ToLower())
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
        }

        return type;
    }

    protected override bool determineParameterType(Type type, out NpgsqlDbType dbType)
    {
        var npgsqlDbType = ResolveNpgsqlDbType(type);
        if (npgsqlDbType != null)
        {
            {
                dbType = npgsqlDbType.Value;
                return true;
            }
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

            {
                dbType = NpgsqlDbType.Array | ToParameterType(type.GetElementType()!);
                return true;
            }
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

    private Type GetNullableType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type.IsValueType)
        {
            return typeof(Nullable<>).MakeGenericType(type);
        }

        return type;
    }

    public void AddTimespanTypes(NpgsqlDbType npgsqlDbType, params Type[] types)
    {
        var timespanTypesList = npgsqlDbType == NpgsqlDbType.Timestamp ? TimespanTypes : TimespanZTypes;
        var typesWithNullables = types.Union(types.Select(GetNullableType))
            .Where(t => !timespanTypesList.Contains(t)).ToList();

        timespanTypesList.AddRange(typesWithNullables);

        ContainmentOperatorTypes.AddRange(typesWithNullables);
    }

    public override string ToQualifiedName(string objectName) =>
        !UseCaseSensitiveQualifiedNames || string.IsNullOrEmpty(objectName) || objectName[0] == '"' || objectName[^1] == '"'
            ? objectName
            : $"\"{objectName}\"";

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new PostgresqlObjectName(schemaName, objectName, ToQualifiedName(schemaName, objectName));
}
