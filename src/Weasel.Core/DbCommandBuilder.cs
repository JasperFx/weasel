using System.Data;
using System.Data.Common;
using JasperFx.Core;
using JasperFx.Core.Reflection;

namespace Weasel.Core;

/// <summary>
///     CommandBuilder for generic DbCommand or DbConnection commands
/// </summary>
public class DbCommandBuilder: CommandBuilderBase<DbCommand, DbParameter, DbType>
{
    public DbCommandBuilder(DbCommand command): base(DbDatabaseProvider.Instance, '@', command)
    {
    }

    public DbCommandBuilder(DbConnection connection): base(DbDatabaseProvider.Instance, '@', connection.CreateCommand())
    {
    }
}

public static class DbCommandBuilderExtensions
{
    /// <summary>
    ///     Compile and execute the batched command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteNonQueryAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the batched command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        DbTransaction? tx,
        CancellationToken ct = default
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        return cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<DbDataReader> ExecuteReaderAsync(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteReaderAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<DbDataReader> ExecuteReaderAsync(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        DbTransaction? tx,
        CancellationToken ct = default
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        return cmd.ExecuteReaderAsync(ct);
    }

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="transform"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default
    ) => connection.FetchListAsync(commandBuilder, transform, null, ct);

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="transform"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static async Task<IReadOnlyList<T>> FetchListAsync<T>(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        DbTransaction? tx,
        CancellationToken ct = default
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        var list = new List<T>();

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            list.Add(await transform(reader, ct).ConfigureAwait(false));
        }

        return list;
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

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new DbObjectName(schemaName, objectName);
}
