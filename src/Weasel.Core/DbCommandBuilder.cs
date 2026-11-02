using System.Data;
using System.Data.Common;
using ImTools;
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

    /// <summary>
    ///     Build against a dialect whose bind marker is not <c>@</c> — Oracle's is <c>:</c>, for
    ///     example. Lets a database-agnostic consumer keep using <see cref="DbCommandBuilder" />
    ///     against such a provider instead of emitting SQL the driver will reject.
    /// </summary>
    /// <param name="command"></param>
    /// <param name="parameterPrefix"></param>
    protected DbCommandBuilder(DbCommand command, char parameterPrefix)
        : base(DbDatabaseProvider.Instance, parameterPrefix, command)
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
    ) => CommandBuilderExtensions.ExecuteNonQueryAsync(connection, commandBuilder, tx, ct);

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
    ) => CommandBuilderExtensions.ExecuteReaderAsync(connection, commandBuilder, tx, ct);

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
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this DbConnection connection,
        DbCommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        DbTransaction? tx,
        CancellationToken ct = default
    ) => CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}

internal class DbDatabaseProvider: DatabaseProvider<DbCommand, DbParameter, DbType>
{
    public static readonly DbDatabaseProvider Instance = new();

    public DbDatabaseProvider(): base(null!)
    {
    }

    protected override void storeMappings()
    {
        store<string>(DbType.String, "varchar(100)");
        store<bool>(DbType.Boolean, "bit");
        store<byte>(DbType.Byte, "tinyint");
        store<short>(DbType.Int16, "smallint");
        store<int>(DbType.Int32, "int");
        store<long>(DbType.Int64, "bigint");
        store<byte[]>(DbType.Binary, "binary");

        // DbType.Date drops the time of day -- SqlClient sends it as `date`, so a bound
        // DateTime came back midnight (weasel#759). DateTime2 is the only one of the three
        // candidates that round trips exactly, and it seeks against both a `datetime` and a
        // `datetime2` column. The database type stays `datetime` deliberately: the column a
        // migration generates is unchanged, and the comparison is still seek-clean.
        store<DateTime>(DbType.DateTime2, "datetime");
        store<DateTimeOffset>(DbType.DateTimeOffset, "datetimeoffset");
        store<DateOnly>(DbType.Date, "date");
        store<TimeOnly>(DbType.Time, "time");
        store<TimeSpan>(DbType.Time, "time");

        store<decimal>(DbType.Decimal, "decimal");
        store<double>(DbType.Double, "float");
        store<float>(DbType.Single, "real");
        store<char>(DbType.String, "nchar(1)");

        // Without this, Guid resolved to DbType.Object, which SqlClient sends as sql_variant.
        // Comparing a sql_variant against a uniqueidentifier column converts the *column* on
        // every row, so `where id = @p` scanned instead of seeking (weasel#758). DbType.Guid
        // is carried correctly by every driver this builder wraps: SqlClient binds
        // uniqueidentifier, Npgsql uuid, MySqlConnector its own Guid, and Microsoft.Data.Sqlite
        // still TEXT, matching how Weasel.Sqlite stores one.
        store<Guid>(DbType.Guid, "uniqueidentifier");
    }

    protected override bool determineParameterType(Type type, out DbType dbType)
    {
        var resolveSqlDbType = ResolveSqlDbType(type);
        if (resolveSqlDbType != null)
        {
            dbType = resolveSqlDbType.Value;
            return true;
        }

        if (type.IsNullable())
        {
            dbType = ToParameterType(type.GetInnerTypeFromNullable());
            return true;
        }

        // Enums and arrays keep the catch-all they have always resolved to. Reviving the
        // Int32 this branch used to carry, and the array refusal below, are behaviour changes
        // of their own rather than part of weasel#758; they are tracked separately.
        if (type.IsEnum || type.IsArray)
        {
            dbType = DbType.Object;
            return true;
        }

        if (type == typeof(DBNull))
        {
            dbType = DbType.Object;
            return true;
        }

        if (type.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
            return true;
        }

        dbType = DbType.Object;
        return false;
    }

    /// <summary>
    ///     The parameter type for a CLR type, or null when this provider has no mapping for it.
    /// </summary>
    /// <remarks>
    ///     This used to end in <c>?? DbType.Object</c>, which could never return null -- so
    ///     <see cref="determineParameterType" /> always reported success and every branch below
    ///     its first was dead code. That made weasel#404's "null means unmapped, defer to the
    ///     driver" unreachable here: an unmapped type was typed <c>Object</c>, which SqlClient
    ///     sends as sql_variant and compares by converting the column (weasel#758).
    /// </remarks>
    private DbType? ResolveSqlDbType(Type type) => ResolveParameterTypeFromMemo(type);

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

        if (ResolveDatabaseType(memberType) is { } result)
        {
            return result;
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        return "json";
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

        // Not "Weasel.SqlServer": this is the provider-neutral DbDatabaseProvider in Weasel.Core,
        // so it named the wrong package on every provider but one (weasel#602).
        throw new NotSupportedException(
            $"Weasel does not (yet) support database type mapping to {type.FullNameInCode()}");
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

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        throw new NotSupportedException();
    }
}
