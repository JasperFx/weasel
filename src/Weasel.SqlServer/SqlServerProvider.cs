using System;
using System.Data;
using System.Data.SqlClient;
using Baseline;
using Baseline.ImTools;
using Weasel.Core;

#nullable enable
namespace Weasel.SqlServer
{
    public class SqlServerProvider : DatabaseProvider<SqlCommand, SqlParameter, SqlConnection, SqlTransaction,
        System.Data.SqlDbType, SqlDataReader>
    {
        public static readonly SqlServerProvider Instance = new();

        private SqlServerProvider() : base("dbo")
        {

        }

        protected override void storeMappings()
        {
            store<string>(SqlDbType.VarChar, "varchar(100)");
            store<bool>(System.Data.SqlDbType.Bit, "bit");
            store<long>(System.Data.SqlDbType.BigInt, "bigint");
            store<byte[]>(System.Data.SqlDbType.Binary, "binary");
            store<DateTime>(System.Data.SqlDbType.Date, "datetime");
            store<DateTimeOffset>(System.Data.SqlDbType.DateTimeOffset, "datetimeoffset");
            store<decimal>(System.Data.SqlDbType.Decimal, "decimal");
            store<double>(System.Data.SqlDbType.Float, "float");
            store<int>(System.Data.SqlDbType.Int, "int");
            store<TimeSpan>(System.Data.SqlDbType.Time, "time");
        }


        // Lazily retrieve the CLR type to SqlDbType and PgTypeName mapping from exposed ISqlTypeMapper.Mappings.
        // This is lazily calculated instead of precached because it allows consuming code to register
        // custom Sql mappings prior to execution.
        private string? ResolveDatabaseType(Type type)
        {
            if (DatabaseTypeMemo.Value.TryFind(type, out var value))
            {
                return value;
            }
            
            if (type.IsNullable() && DatabaseTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out string databaseType))
            {
                DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
                return databaseType;
            }

            throw new NotSupportedException($"Weasel.SqlServer does not (yet) support database type mapping to {type.GetFullName()}");
        }

        private System.Data.SqlDbType? ResolveSqlDbType(Type type)
        {
            if (ParameterTypeMemo.Value.TryFind(type, out var value))
            {
                return value;
            }
            
            if (type.IsNullable() && ParameterTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out var parameterType))
            {
                ParameterTypeMemo.Swap(d => d.AddOrUpdate(type, parameterType));
                return parameterType;
            }

            return System.Data.SqlDbType.Variant;
        }


        protected override Type[] determineClrTypesForParameterType(System.Data.SqlDbType dbType)
        {
            return new Type[0];
        }


        public string ConvertSynonyms(string type)
        {
            switch (type.ToLower())
            {
                case "text":
                case "varchar":
                    return "varchar";

                case "boolean":
                case "bool":
                    return "bit";

                case "integer":
                    return "int";

            }

            return type;
        }


        protected override bool determineParameterType(Type type, out System.Data.SqlDbType dbType)
        {
            var SqlDbType = ResolveSqlDbType(type);
            if (SqlDbType != null)
            {
                {
                    dbType = SqlDbType.Value;
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
                dbType = System.Data.SqlDbType.Int;
                return true;
            }

            if (type.IsArray)
            {
                throw new NotSupportedException("Sql Server does not support arrays");
            }

            if (type == typeof(DBNull))
            {
                dbType = System.Data.SqlDbType.Variant;
                return true;
            }

            dbType = System.Data.SqlDbType.Variant;
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

            if (memberType.IsConstructedGenericType)
            {
                var templateType = memberType.GetGenericTypeDefinition();
                return ResolveDatabaseType(templateType) ?? "jsonb";
            }

            return ResolveDatabaseType(memberType) ?? "jsonb";
        }

        public override void AddParameter(SqlCommand command, SqlParameter parameter)
        {
            command.Parameters.Add(parameter);
        }

        public override void SetParameterType(SqlParameter parameter, System.Data.SqlDbType dbType)
        {
            parameter.SqlDbType = dbType;
        }

        public static CascadeAction ReadAction(string description)
        {
            switch (description.ToUpper().Trim())
            {
                case "CASCADE":
                    return CascadeAction.Cascade;
                case "NO_ACTION":
                    return CascadeAction.NoAction;
                case "SET_NULL":
                    return CascadeAction.SetNull;
                case "SET_DEFAULT":
                    return CascadeAction.SetDefault;
                
            }

            return CascadeAction.NoAction;
        }

    }
}