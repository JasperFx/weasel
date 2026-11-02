using System.Data;
using ImTools;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer;

public class SqlServerProvider: DatabaseProvider<SqlCommand, SqlParameter, SqlDbType>
{
    public const string EngineName = "SqlServer";
    public static readonly SqlServerProvider Instance = new();

    private SqlServerProvider(): base("dbo")
    {
    }

    protected override void storeMappings()
    {
        // Anything missing from this table fell to the Variant catch-all, and SQL Server
        // compares a sql_variant parameter by converting the *column* on every row -- so a
        // `where v = @p` against an indexed column scanned instead of seeking. byte, short,
        // float and char were all missing, which made each of them weasel#758 one type over.
        store<string>(SqlDbType.VarChar, "varchar(100)");
        store<bool>(SqlDbType.Bit, "bit");
        store<byte>(SqlDbType.TinyInt, "tinyint");
        store<short>(SqlDbType.SmallInt, "smallint");
        store<int>(SqlDbType.Int, "int");
        store<long>(SqlDbType.BigInt, "bigint");
        store<byte[]>(SqlDbType.Binary, "binary");

        // SqlDbType.Date sends the value as `date`, which silently dropped the time of day
        // (weasel#759). DateTime2 round trips exactly and still seeks against a `datetime`
        // column as well as a `datetime2` one. The database type stays `datetime` on purpose:
        // the column a migration generates does not change, so nobody gets a schema diff.
        store<DateTime>(SqlDbType.DateTime2, "datetime");
        store<DateTimeOffset>(SqlDbType.DateTimeOffset, "datetimeoffset");
        store<DateOnly>(SqlDbType.Date, "date");
        store<TimeOnly>(SqlDbType.Time, "time");
        store<TimeSpan>(SqlDbType.Time, "time");

        store<decimal>(SqlDbType.Decimal, "decimal");
        store<double>(SqlDbType.Float, "float");
        store<float>(SqlDbType.Real, "real");

        // Unicode, unlike the non-Unicode default `string` carries, because a char had no
        // mapping at all before and so there is no behaviour here to preserve -- and nchar
        // cannot lose a character. The column type generated below agrees with it either way.
        store<char>(SqlDbType.NChar, "nchar(1)");

        store<Guid>(SqlDbType.UniqueIdentifier, "uniqueidentifier");
    }


    // Lazily retrieve the CLR type to SqlDbType and PgTypeName mapping from exposed ISqlTypeMapper.Mappings.
    // This is lazily calculated instead of precached because it allows consuming code to register
    // custom Sql mappings prior to execution.
    private string? ResolveDatabaseType(Type type)
    {
        return ResolveDatabaseTypeFromMemo(type)
               ?? throw new NotSupportedException(
                   $"Weasel.SqlServer does not (yet) support database type mapping to {type.FullNameInCode()}");
    }

    /// <summary>
    ///     The <see cref="SqlDbType" /> for a CLR type, or null when there is no mapping for it.
    /// </summary>
    /// <remarks>
    ///     This used to end in <c>?? SqlDbType.Variant</c>. That could never return null, so
    ///     <see cref="determineParameterType" /> always reported success and every branch after
    ///     its first was unreachable -- which is why weasel#404's "null means unmapped, defer to
    ///     the driver" never applied here. An unmapped type was typed <c>Variant</c>, and SQL
    ///     Server compares a sql_variant parameter by converting the *column* on every row, so
    ///     the query scanned (weasel#758).
    /// </remarks>
    private SqlDbType? ResolveSqlDbType(Type type)
    {
        return ResolveParameterTypeFromMemo(type);
    }


    protected override Type[] determineClrTypesForParameterType(SqlDbType dbType)
    {
        return Type.EmptyTypes;
    }

    /// <inheritdoc />
    public override IdentifierRules Rules => SqlServerIdentifierRules.Instance;

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new SqlServerObjectName(schemaName, objectName);

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        builder.ApplicationName = applicationName;
        return builder.ConnectionString;
    }

    public string ConvertSynonyms(string type)
    {
        switch (type.ToLowerInvariant())
        {
            case "text":
            case "varchar":
                return "varchar";

            case "boolean":
            case "bool":
                return "bit";

            case "integer":
                return "int";

            // rowversion is the modern name; the catalog reports the legacy
            // name timestamp for the same type
            case "rowversion":
                return "timestamp";
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

        // Measured against a live server rather than reasoned about: what every provider did here
        // was wrong against the column its own GetDatabaseType generates, and three of the six did
        // not merely convert but threw. An enum now resolves through EnumStorage.AsInteger -- the
        // JasperFx default, and what PostgreSQL, Firebird and SQLite already did in effect -- which
        // also routes it through the enum's *underlying* type, so a long-backed enum is not
        // truncated. An AsString consumer calls ToParameterType(type, EnumStorage.AsString).
        // weasel#761.
        //
        // On SQL Server specifically, Variant was not a slow answer but a broken one: binding an
        // enum as sql_variant throws "Implicit conversion from data type sql_variant to int is not
        // allowed" against an int column and the varchar equivalent against a character one. There
        // was no behaviour here to preserve.
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
        switch (description.ToUpperInvariant().Trim())
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
