using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Baseline;
using Baseline.ImTools;
using Npgsql;
using Npgsql.TypeMapping;
using NpgsqlTypes;

#nullable enable
namespace Weasel.Postgresql
{
    public class TypeMappings
    {
        public static readonly TypeMappings Instance = new TypeMappings();
        
        private readonly Ref<ImHashMap<Type, string>> PgTypeMemo;
        private readonly Ref<ImHashMap<Type, NpgsqlDbType?>> NpgsqlDbTypeMemo;
        private readonly Ref<ImHashMap<NpgsqlDbType, Type[]>> TypeMemo;

        public List<Type> ContainmentOperatorTypes { get; } = new List<Type>();
        public List<Type> TimespanTypes { get; } = new List<Type>();
        public List<Type> TimespanZTypes { get; } = new List<Type>();

        private TypeMappings()
        {
            // Initialize PgTypeMemo with Types which are not available in Npgsql mappings
            PgTypeMemo = Ref.Of(ImHashMap<Type, string>.Empty);

            PgTypeMemo.Swap(d => d.AddOrUpdate(typeof(long), "bigint"));
            PgTypeMemo.Swap(d => d.AddOrUpdate(typeof(Guid), "uuid"));
            PgTypeMemo.Swap(d => d.AddOrUpdate(typeof(string), "varchar"));
            PgTypeMemo.Swap(d => d.AddOrUpdate(typeof(float), "decimal"));

            // Default Npgsql mapping is 'numeric' but we are using 'decimal'
            PgTypeMemo.Swap(d => d.AddOrUpdate(typeof(decimal), "decimal"));

            // Default Npgsql mappings is 'timestamp' but we are using 'timestamp without time zone'
            PgTypeMemo.Swap(d => d.AddOrUpdate(typeof(DateTime), "timestamp without time zone"));

            NpgsqlDbTypeMemo = Ref.Of(ImHashMap<Type, NpgsqlDbType?>.Empty);

            TypeMemo = Ref.Of(ImHashMap<NpgsqlDbType, Type[]>.Empty);

            AddTimespanTypes(NpgsqlDbType.Timestamp, ResolveTypes(NpgsqlDbType.Timestamp));
            AddTimespanTypes(NpgsqlDbType.TimestampTz, ResolveTypes(NpgsqlDbType.TimestampTz));

            RegisterMapping(typeof(uint), "oid", NpgsqlDbType.Oid);
        }

        public void RegisterMapping(Type type, string pgType, NpgsqlDbType? npgsqlDbType)
        {
            PgTypeMemo.Swap(d => d.AddOrUpdate(type, pgType));
            NpgsqlDbTypeMemo.Swap(d => d.AddOrUpdate(type, npgsqlDbType));
        }

        // Lazily retrieve the CLR type to NpgsqlDbType and PgTypeName mapping from exposed INpgsqlTypeMapper.Mappings.
        // This is lazily calculated instead of precached because it allows consuming code to register
        // custom npgsql mappings prior to execution.
        private string? ResolvePgType(Type type)
        {
            if (PgTypeMemo.Value.TryFind(type, out var value))
                return value;

            value = GetTypeMapping(type)?.PgTypeName;

            PgTypeMemo.Swap(d => d.AddOrUpdate(type, value));

            return value;
        }

        private NpgsqlDbType? ResolveNpgsqlDbType(Type type)
        {
            if (NpgsqlDbTypeMemo.Value.TryFind(type, out var value))
                return value;

            value = GetTypeMapping(type)?.NpgsqlDbType;

            NpgsqlDbTypeMemo.Swap(d => d.AddOrUpdate(type, value));

            return value;
        }

        public Type[] ResolveTypes(NpgsqlDbType npgsqlDbType)
        {
            if (TypeMemo.Value.TryFind(npgsqlDbType, out var values))
                return values;

            values = GetTypeMapping(npgsqlDbType)?.ClrTypes;

            TypeMemo.Swap(d => d.AddOrUpdate(npgsqlDbType, values!));

            return values!;
        }

        private NpgsqlTypeMapping? GetTypeMapping(Type type)
            => NpgsqlConnection
                .GlobalTypeMapper
                .Mappings
                .FirstOrDefault(mapping => mapping.ClrTypes.Contains(type));

        private NpgsqlTypeMapping? GetTypeMapping(NpgsqlDbType type)
            => NpgsqlConnection
                .GlobalTypeMapper
                .Mappings
                .FirstOrDefault(mapping => mapping.NpgsqlDbType == type);

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


        /// <summary>
        /// Some portion of implementation adapted from Npgsql GlobalTypeMapper.ToNpgsqlDbType(Type type)
        /// https://github.com/npgsql/npgsql/blob/dev/src/Npgsql/TypeMapping/GlobalTypeMapper.cs
        /// Possibly this method can be trimmed down when Npgsql eventually exposes ToNpgsqlDbType
        /// </summary>
        public NpgsqlDbType ToDbType(Type type)
        {
            if (determineNpgsqlDbType(type, out var dbType))
                return dbType;

            throw new NotSupportedException("Can't infer NpgsqlDbType for type " + type);
        }

        public NpgsqlDbType? TryGetDbType(Type? type)
        {
            if (type == null || !determineNpgsqlDbType(type, out var dbType))
                return null;

            return dbType;
        }

        private bool determineNpgsqlDbType(Type type, out NpgsqlDbType dbType)
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
                dbType = ToDbType(type.GetInnerTypeFromNullable());
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
                    dbType = NpgsqlDbType.Array | ToDbType(type.GetElementType());
                    return true;
                }
            }

            var typeInfo = type.GetTypeInfo();

            var ilist = typeInfo.ImplementedInterfaces.FirstOrDefault(x =>
                x.GetTypeInfo().IsGenericType && x.GetGenericTypeDefinition() == typeof(IList<>));
            if (ilist != null)
            {
                dbType = NpgsqlDbType.Array | ToDbType(ilist.GetGenericArguments()[0]);
                return true;
            }

            if (typeInfo.IsGenericType && type.GetGenericTypeDefinition() == typeof(NpgsqlRange<>))
            {
                dbType = NpgsqlDbType.Range | ToDbType(type.GetGenericArguments()[0]);
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

        public string GetPgType(Type memberType, EnumStorage enumStyle)
        {
            if (memberType.IsEnum)
            {
                return enumStyle == EnumStorage.AsInteger ? "integer" : "varchar";
            }

            if (memberType.IsArray)
            {
                return GetPgType(memberType.GetElementType()!, enumStyle) + "[]";
            }

            if (memberType.IsNullable())
            {
                return GetPgType(memberType.GetInnerTypeFromNullable(), enumStyle);
            }

            if (memberType.IsConstructedGenericType)
            {
                var templateType = memberType.GetGenericTypeDefinition();
                return ResolvePgType(templateType) ?? "jsonb";
            }

            return ResolvePgType(memberType) ?? "jsonb";
        }

        public bool HasTypeMapping(Type memberType)
        {
            if (memberType.IsNullable())
            {
                return HasTypeMapping(memberType.GetInnerTypeFromNullable());
            }

            // more complicated later
            return ResolvePgType(memberType) != null || memberType.IsEnum;
        }

        private Type GetNullableType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsValueType)
                return typeof(Nullable<>).MakeGenericType(type);
            else
                return type;
        }

        public void AddTimespanTypes(NpgsqlDbType npgsqlDbType, params Type[] types)
        {
            var timespanTypesList = (npgsqlDbType == NpgsqlDbType.Timestamp) ? TimespanTypes : TimespanZTypes;
            var typesWithNullables = types.Union(types.Select(t => GetNullableType(t))).Where(t => !timespanTypesList.Contains(t)).ToList();

            timespanTypesList.AddRange(typesWithNullables);

            ContainmentOperatorTypes.AddRange(typesWithNullables);
        }
    }
}
