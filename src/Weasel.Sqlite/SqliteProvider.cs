using System.Data;
using ImTools;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.Data.Sqlite;
using Weasel.Core;

namespace Weasel.Sqlite;

public class SqliteProvider: DatabaseProvider<SqliteCommand, SqliteParameter, SqliteType>
{
    public const string EngineName = "SQLite";
    public static readonly SqliteProvider Instance = new();

    private SqliteProvider(): base("main")
    {
    }

    protected override void storeMappings()
    {
        // SQLite type system: INTEGER, REAL, TEXT, BLOB, NULL
        // We map .NET types to both SqliteType and the database type string

        // Integer types
        store<bool>(SqliteType.Integer, "INTEGER");
        store<byte>(SqliteType.Integer, "INTEGER");
        store<short>(SqliteType.Integer, "INTEGER");
        store<int>(SqliteType.Integer, "INTEGER");
        store<long>(SqliteType.Integer, "INTEGER");

        // Real/floating point types
        store<float>(SqliteType.Real, "REAL");
        store<double>(SqliteType.Real, "REAL");
        store<decimal>(SqliteType.Real, "REAL");

        // Text types
        store<string>(SqliteType.Text, "TEXT");
        store<char>(SqliteType.Text, "TEXT");
        store<DateTime>(SqliteType.Text, "TEXT"); // ISO8601 format
        store<DateTimeOffset>(SqliteType.Text, "TEXT"); // ISO8601 with timezone
        store<Guid>(SqliteType.Text, "TEXT"); // Stored as string
        store<TimeSpan>(SqliteType.Text, "TEXT"); // Stored as string

        // Blob types
        store<byte[]>(SqliteType.Blob, "BLOB");
    }

    private string? ResolveDatabaseType(Type type)
    {
        // Unmapped types fall through to TEXT in GetDatabaseType (for JSON storage),
        // so we just return null on a memo miss and let the caller decide.
        return ResolveDatabaseTypeFromMemo(type);
    }

    private SqliteType? ResolveSqliteType(Type type)
    {
        return ResolveParameterTypeFromMemo(type) ?? SqliteType.Text;
    }

    protected override Type[] determineClrTypesForParameterType(SqliteType dbType)
    {
        // SQLite has flexible type affinity, multiple .NET types can map to same SQLite type
        return dbType switch
        {
            SqliteType.Integer => new[] { typeof(bool), typeof(byte), typeof(short), typeof(int), typeof(long) },
            SqliteType.Real => new[] { typeof(float), typeof(double), typeof(decimal) },
            SqliteType.Text => new[] { typeof(string), typeof(char), typeof(DateTime), typeof(DateTimeOffset), typeof(Guid), typeof(TimeSpan) },
            SqliteType.Blob => new[] { typeof(byte[]) },
            _ => Type.EmptyTypes
        };
    }

    public string ConvertSynonyms(string type)
    {
        // Normalize type names to SQLite standard types
        switch (type.ToLowerInvariant().Trim())
        {
            case "int":
            case "integer":
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "bigint":
            case "unsigned big int":
            case "int2":
            case "int8":
            case "boolean":
            case "bool":
                return "INTEGER";

            case "character":
            case "varchar":
            case "varying character":
            case "nchar":
            case "native character":
            case "nvarchar":
            case "text":
            case "clob":
            case "string":
            case "datetime":
            case "date":
            case "timestamp":
                return "TEXT";

            case "double":
            case "double precision":
            case "float":
            case "real":
                return "REAL";

            // NUMERIC and DECIMAL are not REAL. SQLite gives a declared type REAL affinity only
            // when it contains REAL, FLOA or DOUB; NUMERIC and DECIMAL get NUMERIC affinity, which
            // stores a whole number as an integer. Mapping them to REAL changed stored values --
            // an id declared numeric came back as 1.0 rather than 1.
            case "numeric":
            case "decimal":
                return "NUMERIC";

            case "blob":
            case "binary":
            case "varbinary":
                return "BLOB";
        }

        // Return as-is if no synonym found (could be custom type)
        return type;
    }

    /// <summary>
    ///     The type as a STRICT table has to declare it. STRICT accepts only INT, INTEGER, REAL,
    ///     TEXT, BLOB and ANY, so anything else must be mapped before it reaches the DDL or SQLite
    ///     rejects the CREATE outright with <c>unknown datatype</c>.
    /// </summary>
    /// <remarks>
    ///     Two kinds of type reach here that <see cref="ConvertSynonyms" /> does not resolve to a
    ///     legal one on its own. A parameterized type never matches its switch and came back
    ///     unchanged, so <c>VARCHAR(255)</c> was emitted verbatim and rejected; the length is
    ///     stripped first. And NUMERIC -- which <c>numeric</c> and <c>decimal</c> map to since
    ///     weasel#533 -- has no STRICT equivalent at all.
    ///
    ///     NUMERIC becomes ANY rather than REAL because ANY is the one STRICT type that stores a
    ///     whole number as an integer instead of converting it to a float, and that conversion is
    ///     exactly what #533 exists to prevent. The cost is that ANY enforces nothing for that
    ///     column, which is a real trade against the point of a STRICT table; REAL is the
    ///     alternative if enforcement matters more than the stored value.
    /// </remarks>
    public string ToStrictType(string declaredType)
    {
        var converted = ConvertSynonyms(declaredType.Split('(')[0].Trim()).ToUpperInvariant();

        return converted switch
        {
            "INT" or "INTEGER" or "REAL" or "TEXT" or "BLOB" or "ANY" => converted,
            _ => "ANY"
        };
    }

    protected override bool determineParameterType(Type type, out SqliteType dbType)
    {
        var sqliteType = ResolveSqliteType(type);
        if (sqliteType != null)
        {
            dbType = sqliteType.Value;
            return true;
        }

        if (type.IsNullable())
        {
            dbType = ToParameterType(type.GetInnerTypeFromNullable());
            return true;
        }

        if (type.IsEnum)
        {
            dbType = SqliteType.Integer;
            return true;
        }

        if (type.IsArray)
        {
            // SQLite doesn't support arrays, but byte[] is handled above
            if (type == typeof(byte[]))
            {
                dbType = SqliteType.Blob;
                return true;
            }
            throw new NotSupportedException("SQLite does not support array column types (except byte[])");
        }

        if (type == typeof(DBNull))
        {
            dbType = SqliteType.Text;
            return true;
        }

        if (type.IsConstructedGenericType)
        {
            dbType = ToParameterType(type.GetGenericTypeDefinition());
            return true;
        }

        // Default to TEXT for complex types (JSON storage)
        dbType = SqliteType.Text;
        return false;
    }

    public override string GetDatabaseType(Type memberType, EnumStorage enumStyle)
    {
        if (memberType.IsEnum)
        {
            return enumStyle == EnumStorage.AsInteger ? "INTEGER" : "TEXT";
        }

        if (memberType.IsNullable())
        {
            return GetDatabaseType(memberType.GetInnerTypeFromNullable(), enumStyle);
        }

        // Check memo first
        if (ResolveDatabaseType(memberType) is { } result)
        {
            return result;
        }

        // byte[] is already handled above
        if (memberType.IsArray)
        {
            throw new NotSupportedException("SQLite does not support array column types (except byte[])");
        }

        if (memberType.IsConstructedGenericType)
        {
            return GetDatabaseType(memberType.GetGenericTypeDefinition(), enumStyle);
        }

        // Default to TEXT for complex types - enables JSON storage via JSON1 extension
        return "TEXT";
    }

    public override void AddParameter(SqliteCommand command, SqliteParameter parameter)
    {
        command.Parameters.Add(parameter);
    }

    public override void SetParameterType(SqliteParameter parameter, SqliteType dbType)
    {
        parameter.SqliteType = dbType;
    }

    /// <inheritdoc />
    public override IdentifierRules Rules => SqliteIdentifierRules.Instance;

    public override DbObjectName Parse(string schemaName, string objectName) =>
        new SqliteObjectName(schemaName, objectName);

    public override string AddApplicationNameToConnectionString(string connectionString, string applicationName)
    {
        // SQLite connection string doesn't have application name parameter
        // We could add it as a comment or custom parameter, but for now just return as-is
        var builder = new SqliteConnectionStringBuilder(connectionString);
        return builder.ConnectionString;
    }

    public static CascadeAction ReadAction(string description)
    {
        switch (description.ToUpperInvariant().Trim())
        {
            case "CASCADE":
                return CascadeAction.Cascade;
            case "NO ACTION":
            case "NO_ACTION":
                return CascadeAction.NoAction;
            case "SET NULL":
            case "SET_NULL":
                return CascadeAction.SetNull;
            case "SET DEFAULT":
            case "SET_DEFAULT":
                return CascadeAction.SetDefault;
            case "RESTRICT":
                return CascadeAction.Restrict;
        }

        return CascadeAction.NoAction;
    }
}
