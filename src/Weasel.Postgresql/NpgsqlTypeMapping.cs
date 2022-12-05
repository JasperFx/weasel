using System.Collections;
using System.Collections.Immutable;
using System.Collections.Specialized;
using System.Data;
using System.Net;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Text.Json;
using NpgsqlTypes;

namespace Weasel.Postgresql;

public class NpgsqlTypeMapping
{
    public NpgsqlTypeMapping(NpgsqlDbType? npgsqlDbType, DbType dbType, string? dataTypeName, Type clrType)
    {
        (NpgsqlDbType, DbType, DataTypeName, ClrTypes) = (npgsqlDbType, dbType, dataTypeName, new[] { clrType });
    }

    public NpgsqlTypeMapping(NpgsqlDbType? npgsqlDbType, DbType dbType, string? dataTypeName, params Type[] clrTypes)
    {
        (NpgsqlDbType, DbType, DataTypeName, ClrTypes) = (npgsqlDbType, dbType, dataTypeName, clrTypes);
    }

    public NpgsqlDbType? NpgsqlDbType { get; }
    public DbType DbType { get; }
    public string? DataTypeName { get; }
    public Type[] ClrTypes { get; }
}

/// <summary>
///     Class defining custom NpgsqlType <=> DbType <=> CLR types
/// </summary>
/// <remarks>
///     Based on
///     https://github.com/npgsql/npgsql/blob/a1d366c5692cc00a5edc1ec5bc9090952c7a63e7/src/Npgsql/TypeMapping/BuiltInTypeHandlerResolver.cs
/// </remarks>
public class NpgsqlTypeMapper
{
    public static readonly List<NpgsqlTypeMapping> Mappings = new()
    {
        // Numeric types
        new NpgsqlTypeMapping(NpgsqlDbType.Smallint, DbType.Int16, "smallint", typeof(short), typeof(byte),
            typeof(sbyte)),
        new NpgsqlTypeMapping(NpgsqlDbType.Integer, DbType.Int32, "integer", typeof(int)),
        new NpgsqlTypeMapping(NpgsqlDbType.Integer, DbType.Int32, "integer", typeof(int)),
        new NpgsqlTypeMapping(NpgsqlDbType.Bigint, DbType.Int64, "bigint", typeof(long)),
        new NpgsqlTypeMapping(NpgsqlDbType.Real, DbType.Single, "real", typeof(float)),
        new NpgsqlTypeMapping(NpgsqlDbType.Double, DbType.Double, "double precision", typeof(double)),
        new NpgsqlTypeMapping(NpgsqlDbType.Numeric, DbType.Decimal, "decimal", typeof(decimal), typeof(BigInteger)),
        new NpgsqlTypeMapping(NpgsqlDbType.Numeric, DbType.Decimal, "decimal", typeof(decimal), typeof(BigInteger)),
        new NpgsqlTypeMapping(NpgsqlDbType.Money, DbType.Int16, "money"),

        // Text types
        new NpgsqlTypeMapping(NpgsqlDbType.Text, DbType.String, "text", typeof(string), typeof(char[]), typeof(char),
            typeof(ArraySegment<char>)),
        new NpgsqlTypeMapping(NpgsqlDbType.Xml, DbType.Xml, "xml"),
        new NpgsqlTypeMapping(NpgsqlDbType.Varchar, DbType.String, "character varying"),
        new NpgsqlTypeMapping(NpgsqlDbType.Varchar, DbType.String, "character varying"),
        new NpgsqlTypeMapping(NpgsqlDbType.Char, DbType.String, "character"),
        new NpgsqlTypeMapping(NpgsqlDbType.Name, DbType.String, "name"),
        new NpgsqlTypeMapping(NpgsqlDbType.Refcursor, DbType.String, "refcursor"),
        new NpgsqlTypeMapping(NpgsqlDbType.Citext, DbType.String, "citext"),
        new NpgsqlTypeMapping(NpgsqlDbType.Jsonb, DbType.Object, "jsonb", typeof(JsonDocument)),
        new NpgsqlTypeMapping(NpgsqlDbType.Json, DbType.Object, "json"),
        new NpgsqlTypeMapping(NpgsqlDbType.JsonPath, DbType.Object, "jsonpath"),

        // Date/time types
#pragma warning disable 618 // NpgsqlDateTime is obsolete, remove in 7.0
        new NpgsqlTypeMapping(NpgsqlDbType.Timestamp, DbType.DateTime, "timestamp without time zone", typeof(DateTime)),
#pragma warning disable 618
        new NpgsqlTypeMapping(NpgsqlDbType.TimestampTz, DbType.DateTimeOffset, "timestamp with time zone",
            typeof(DateTimeOffset)),
        new NpgsqlTypeMapping(NpgsqlDbType.Date, DbType.Date, "date", typeof(DateOnly)),
        new NpgsqlTypeMapping(NpgsqlDbType.Time, DbType.Time, "time without time zone", typeof(TimeOnly)),
        new NpgsqlTypeMapping(NpgsqlDbType.TimeTz, DbType.Object, "time with time zone"),
        new NpgsqlTypeMapping(NpgsqlDbType.Interval, DbType.Object, "interval", typeof(TimeSpan)),
        new NpgsqlTypeMapping(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, DbType.Object,
            "timestamp without time zone[]"),
        new NpgsqlTypeMapping(NpgsqlDbType.Array | NpgsqlDbType.TimestampTz, DbType.Object,
            "timestamp with time zone[]"),
        new NpgsqlTypeMapping(NpgsqlDbType.Range | NpgsqlDbType.Timestamp, DbType.Object, "tsrange"),
        new NpgsqlTypeMapping(NpgsqlDbType.Range | NpgsqlDbType.TimestampTz, DbType.Object, "tstzrange"),
        new NpgsqlTypeMapping(NpgsqlDbType.Multirange | NpgsqlDbType.Timestamp, DbType.Object, "tsmultirange"),
        new NpgsqlTypeMapping(NpgsqlDbType.Multirange | NpgsqlDbType.TimestampTz, DbType.Object, "tstzmultirange"),

        // Network types
        new NpgsqlTypeMapping(NpgsqlDbType.Cidr, DbType.Object, "cidr"),
#pragma warning disable 618
        new NpgsqlTypeMapping(NpgsqlDbType.Inet, DbType.Object, "inet", typeof(IPAddress),
            typeof((IPAddress Address, int Subnet)), typeof(NpgsqlInet), IPAddress.Loopback.GetType()),
#pragma warning restore 618
        new NpgsqlTypeMapping(NpgsqlDbType.MacAddr, DbType.Object, "macaddr", typeof(PhysicalAddress)),
        new NpgsqlTypeMapping(NpgsqlDbType.MacAddr8, DbType.Object, "macaddr8"),

        // Full-text search types
        new NpgsqlTypeMapping(NpgsqlDbType.TsQuery, DbType.Object, "tsquery",
            typeof(NpgsqlTsQuery), typeof(NpgsqlTsQueryAnd), typeof(NpgsqlTsQueryEmpty),
            typeof(NpgsqlTsQueryFollowedBy),
            typeof(NpgsqlTsQueryLexeme), typeof(NpgsqlTsQueryNot), typeof(NpgsqlTsQueryOr), typeof(NpgsqlTsQueryBinOp)
        ),
        new NpgsqlTypeMapping(NpgsqlDbType.TsVector, DbType.Object, "tsvector", typeof(NpgsqlTsVector)),

        // Geometry types
        new NpgsqlTypeMapping(NpgsqlDbType.Box, DbType.Object, "box", typeof(NpgsqlBox)),
        new NpgsqlTypeMapping(NpgsqlDbType.Circle, DbType.Object, "circle", typeof(NpgsqlCircle)),
        new NpgsqlTypeMapping(NpgsqlDbType.Line, DbType.Object, "line", typeof(NpgsqlLine)),
        new NpgsqlTypeMapping(NpgsqlDbType.LSeg, DbType.Object, "lseg", typeof(NpgsqlLSeg)),
        new NpgsqlTypeMapping(NpgsqlDbType.Path, DbType.Object, "path", typeof(NpgsqlPath)),
        new NpgsqlTypeMapping(NpgsqlDbType.Point, DbType.Object, "point", typeof(NpgsqlPoint)),
        new NpgsqlTypeMapping(NpgsqlDbType.Polygon, DbType.Object, "polygon", typeof(NpgsqlPolygon)),

        // LTree types
        new NpgsqlTypeMapping(NpgsqlDbType.LQuery, DbType.Object, "lquery"),
        new NpgsqlTypeMapping(NpgsqlDbType.LTree, DbType.Object, "ltree"),
        new NpgsqlTypeMapping(NpgsqlDbType.LTxtQuery, DbType.Object, "ltxtquery"),

        // UInt types
        new NpgsqlTypeMapping(NpgsqlDbType.Oid, DbType.Object, "oid"),
        new NpgsqlTypeMapping(NpgsqlDbType.Xid, DbType.Object, "xid"),
        new NpgsqlTypeMapping(NpgsqlDbType.Xid8, DbType.Object, "xid8"),
        new NpgsqlTypeMapping(NpgsqlDbType.Cid, DbType.Object, "cid"),
        new NpgsqlTypeMapping(NpgsqlDbType.Regtype, DbType.Object, "regtype"),
        new NpgsqlTypeMapping(NpgsqlDbType.Regconfig, DbType.Object, "regconfig"),

        // Misc types
        new NpgsqlTypeMapping(NpgsqlDbType.Boolean, DbType.Boolean, "boolean", typeof(bool)),
        new NpgsqlTypeMapping(NpgsqlDbType.Boolean, DbType.Boolean, "boolean", typeof(bool)),
        new NpgsqlTypeMapping(NpgsqlDbType.Bytea, DbType.Binary, "bytea", typeof(byte[]), typeof(ArraySegment<byte>),
            typeof(ReadOnlyMemory<byte>), typeof(Memory<byte>)),
        new NpgsqlTypeMapping(NpgsqlDbType.Uuid, DbType.Guid, "uuid", typeof(Guid)),
        new NpgsqlTypeMapping(NpgsqlDbType.Varbit, DbType.Object, "bit varying", typeof(BitArray), typeof(BitVector32)),
        new NpgsqlTypeMapping(NpgsqlDbType.Varbit, DbType.Object, "bit varying", typeof(BitArray), typeof(BitVector32)),
        new NpgsqlTypeMapping(NpgsqlDbType.Bit, DbType.Object, "bit"),
        new NpgsqlTypeMapping(NpgsqlDbType.Hstore, DbType.Object, "hstore", typeof(Dictionary<string, string?>),
            typeof(IDictionary<string, string?>), typeof(ImmutableDictionary<string, string?>)),

        // Internal types
        new NpgsqlTypeMapping(NpgsqlDbType.Int2Vector, DbType.Object, "int2vector"),
        new NpgsqlTypeMapping(NpgsqlDbType.Oidvector, DbType.Object, "oidvector"),
        new NpgsqlTypeMapping(NpgsqlDbType.PgLsn, DbType.Object, "pg_lsn", typeof(NpgsqlLogSequenceNumber)),
        new NpgsqlTypeMapping(NpgsqlDbType.Tid, DbType.Object, "tid", typeof(NpgsqlTid)),
        new NpgsqlTypeMapping(NpgsqlDbType.InternalChar, DbType.Object, "char"),

        // Special types
        new NpgsqlTypeMapping(NpgsqlDbType.Unknown, DbType.Object, "unknown")
    };
}
