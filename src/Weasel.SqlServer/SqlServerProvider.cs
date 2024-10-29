using System.Data;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer;

public class SqlServerProvider: DatabaseProvider<SqlCommand, SqlParameter, SqlDbType>
{
    public static readonly SqlServerProvider Instance = new();

    private SqlServerProvider(): base("dbo")
    {
    }

    protected override void storeMappings()
    {
        store<string>(SqlDbType.VarChar, "varchar(100)");
        store<bool>(SqlDbType.Bit, "bit");
        store<long>(SqlDbType.BigInt, "bigint");
        store<byte[]>(SqlDbType.Binary, "binary");
        store<DateTime>(SqlDbType.Date, "datetime");
        store<DateTimeOffset>(SqlDbType.DateTimeOffset, "datetimeoffset");
        store<decimal>(SqlDbType.Decimal, "decimal");
        store<double>(SqlDbType.Float, "float");
        store<int>(SqlDbType.Int, "int");
        store<TimeSpan>(SqlDbType.Time, "time");
        store<Guid>(SqlDbType.UniqueIdentifier, "uniqueidentifier");
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

        if (!type.IsNullable() ||
            !DatabaseTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out var databaseType))
            throw new NotSupportedException(
                $"Weasel.SqlServer does not (yet) support database type mapping to {type.FullNameInCode()}");

        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
        return databaseType;

    }

    private SqlDbType? ResolveSqlDbType(Type type)
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

        return SqlDbType.Variant;
    }


    protected override Type[] determineClrTypesForParameterType(SqlDbType dbType)
    {
        return Type.EmptyTypes;
    }

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new SqlServerObjectName(schemaName, objectName);

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


    protected override bool determineParameterType(Type type, out SqlDbType dbType)
    {
        var SqlDbType = ResolveSqlDbType(type);
        if (SqlDbType != null)
        {
            dbType = SqlDbType.Value;
            return true;
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

        if (type.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
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

        if (ResolveDatabaseType(memberType) is { } result)
        {
            return result;
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        return "nvarchar(max)";
    }

    public override void AddParameter(SqlCommand command, SqlParameter parameter)
    {
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(SqlParameter parameter, SqlDbType dbType)
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
