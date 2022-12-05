using System.Data;
using System.Data.Common;
using JasperFx.Core.Reflection;
using Weasel.Core.Util;

namespace Weasel.Core;

/// <summary>
///     CommandBuilder for generic DbCommand or DbConnection commands
/// </summary>
public class
    DbCommandBuilder: CommandBuilderBase<DbCommand, DbParameter, DbConnection, DbTransaction, DbType, DbDataReader>
{
    public DbCommandBuilder(DbCommand command): base(DbDatabaseProvider.Instance, '@', command)
    {
    }

    public DbCommandBuilder(DbConnection connection): base(DbDatabaseProvider.Instance, '@', connection.CreateCommand())
    {
    }
}

internal class DbDatabaseProvider: DatabaseProvider<DbCommand, DbParameter, DbConnection, DbTransaction, DbType,
    DbDataReader>
{
    public static readonly DbDatabaseProvider Instance = new();

    public DbDatabaseProvider(): base(null!)
    {
    }

    protected override void storeMappings()
    {
        store<string>(DbType.String, "varchar(100)");
        store<bool>(DbType.Boolean, "bit");
        store<long>(DbType.Int64, "bigint");
        store<byte[]>(DbType.Binary, "binary");
        store<DateTime>(DbType.Date, "datetime");
        store<DateTimeOffset>(DbType.DateTimeOffset, "datetimeoffset");
        store<decimal>(DbType.Decimal, "decimal");
        store<double>(DbType.Double, "float");
        store<int>(DbType.Int32, "int");
        store<TimeSpan>(DbType.Time, "time");
    }

    protected override bool determineParameterType(Type type, out DbType dbType)
    {
        var resolveSqlDbType = ResolveSqlDbType(type);
        if (resolveSqlDbType != null)
        {
            {
                dbType = resolveSqlDbType.Value;
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
            dbType = DbType.Int32;
            return true;
        }

        if (type.IsArray)
        {
            throw new NotSupportedException("The generic database provider does not support arrays");
        }

        if (type == typeof(DBNull))
        {
            dbType = DbType.Object;
            return true;
        }

        dbType = DbType.Object;
        return false;
    }

    private DbType? ResolveSqlDbType(Type type)
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

        return DbType.Object;
    }

    protected override Type[] determineClrTypesForParameterType(DbType dbType)
    {
        return Type.EmptyTypes;
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
            return ResolveDatabaseType(templateType) ?? "json";
        }

        return ResolveDatabaseType(memberType) ?? "json";
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

        if (type.IsNullable() &&
            DatabaseTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out string databaseType))
        {
            DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
            return databaseType;
        }

        throw new NotSupportedException(
            $"Weasel.SqlServer does not (yet) support database type mapping to {type.GetFullName()}");
    }

    public override void AddParameter(DbCommand command, DbParameter parameter)
    {
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(DbParameter parameter, DbType dbType)
    {
        parameter.DbType = dbType;
    }
}
