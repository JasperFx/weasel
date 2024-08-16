using System.Collections;
using System.Collections.Immutable;
using System.Collections.Specialized;
using System.Data;
using System.Net;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Text.Json;
using JasperFx.Core;
using NetTopologySuite.Geometries;
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
    public static readonly Cache<NpgsqlDbType, NpgsqlTypeMapping> Mappings = new(new Dictionary<NpgsqlDbType, NpgsqlTypeMapping>
    {
        // Numeric types
        {NpgsqlDbType.Smallint,new NpgsqlTypeMapping(NpgsqlDbType.Smallint, DbType.Int16, "smallint", typeof(short), typeof(byte),
            typeof(sbyte))},
        {NpgsqlDbType.Integer, new NpgsqlTypeMapping(NpgsqlDbType.Integer, DbType.Int32, "integer", typeof(int))},
        {NpgsqlDbType.Bigint, new NpgsqlTypeMapping(NpgsqlDbType.Bigint, DbType.Int64, "bigint", typeof(long))},
        {NpgsqlDbType.Real, new NpgsqlTypeMapping(NpgsqlDbType.Real, DbType.Single, "real", typeof(float))},
        {NpgsqlDbType.Double, new NpgsqlTypeMapping(NpgsqlDbType.Double, DbType.Double, "double precision", typeof(double))},
        {NpgsqlDbType.Numeric, new NpgsqlTypeMapping(NpgsqlDbType.Numeric, DbType.Decimal, "decimal", typeof(decimal), typeof(BigInteger))},
        {NpgsqlDbType.Money, new NpgsqlTypeMapping(NpgsqlDbType.Money, DbType.Int16, "money")},

        // Text types
        {NpgsqlDbType.Text, new NpgsqlTypeMapping(NpgsqlDbType.Text, DbType.String, "text", typeof(string), typeof(char[]), typeof(char),
            typeof(ArraySegment<char>))},
        {NpgsqlDbType.Xml, new NpgsqlTypeMapping(NpgsqlDbType.Xml, DbType.Xml, "xml")},
        {NpgsqlDbType.Varchar, new NpgsqlTypeMapping(NpgsqlDbType.Varchar, DbType.String, "character varying")},
        {NpgsqlDbType.Char, new NpgsqlTypeMapping(NpgsqlDbType.Char, DbType.String, "character")},
        {NpgsqlDbType.Name, new NpgsqlTypeMapping(NpgsqlDbType.Name, DbType.String, "name")},
        {NpgsqlDbType.Refcursor, new NpgsqlTypeMapping(NpgsqlDbType.Refcursor, DbType.String, "refcursor")},
        {NpgsqlDbType.Citext, new NpgsqlTypeMapping(NpgsqlDbType.Citext, DbType.String, "citext")},
        {NpgsqlDbType.Jsonb, new NpgsqlTypeMapping(NpgsqlDbType.Jsonb, DbType.Object, "jsonb", typeof(JsonDocument))},
        {NpgsqlDbType.Json, new NpgsqlTypeMapping(NpgsqlDbType.Json, DbType.Object, "json")},
        {NpgsqlDbType.JsonPath, new NpgsqlTypeMapping(NpgsqlDbType.JsonPath, DbType.Object, "jsonpath")},

        // Date/time types
        {NpgsqlDbType.Timestamp, new NpgsqlTypeMapping(NpgsqlDbType.Timestamp, DbType.DateTime, "timestamp without time zone", typeof(DateTime))},
        {NpgsqlDbType.TimestampTz, new NpgsqlTypeMapping(NpgsqlDbType.TimestampTz, DbType.DateTimeOffset, "timestamp with time zone",
            typeof(DateTimeOffset))},
        {NpgsqlDbType.Date, new NpgsqlTypeMapping(NpgsqlDbType.Date, DbType.Date, "date", typeof(DateOnly))},
        {NpgsqlDbType.Time, new NpgsqlTypeMapping(NpgsqlDbType.Time, DbType.Time, "time without time zone", typeof(TimeOnly))},
        {NpgsqlDbType.TimeTz, new NpgsqlTypeMapping(NpgsqlDbType.TimeTz, DbType.Object, "time with time zone")},
        {NpgsqlDbType.Interval, new NpgsqlTypeMapping(NpgsqlDbType.Interval, DbType.Object, "interval", typeof(TimeSpan))},
        {NpgsqlDbType.Array | NpgsqlDbType.Timestamp, new NpgsqlTypeMapping(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, DbType.Object,
            "timestamp without time zone[]")},
        {NpgsqlDbType.Array | NpgsqlDbType.TimestampTz, new NpgsqlTypeMapping(NpgsqlDbType.Array | NpgsqlDbType.TimestampTz, DbType.Object,
            "timestamp with time zone[]")},
        {NpgsqlDbType.Range | NpgsqlDbType.Timestamp, new NpgsqlTypeMapping(NpgsqlDbType.Range | NpgsqlDbType.Timestamp, DbType.Object, "tsrange")},
        {NpgsqlDbType.Range | NpgsqlDbType.TimestampTz, new NpgsqlTypeMapping(NpgsqlDbType.Range | NpgsqlDbType.TimestampTz, DbType.Object, "tstzrange")},
        {NpgsqlDbType.Multirange | NpgsqlDbType.Timestamp, new NpgsqlTypeMapping(NpgsqlDbType.Multirange | NpgsqlDbType.Timestamp, DbType.Object, "tsmultirange")},
        {NpgsqlDbType.Multirange | NpgsqlDbType.TimestampTz, new NpgsqlTypeMapping(NpgsqlDbType.Multirange | NpgsqlDbType.TimestampTz, DbType.Object, "tstzmultirange")},

        // Network types
        {NpgsqlDbType.Cidr, new NpgsqlTypeMapping(NpgsqlDbType.Cidr, DbType.Object, "cidr")},
        {NpgsqlDbType.Inet, new NpgsqlTypeMapping(NpgsqlDbType.Inet, DbType.Object, "inet", typeof(IPAddress),
            typeof((IPAddress Address, int Subnet)), typeof(NpgsqlInet), IPAddress.Loopback.GetType())},
        {NpgsqlDbType.MacAddr, new NpgsqlTypeMapping(NpgsqlDbType.MacAddr, DbType.Object, "macaddr", typeof(PhysicalAddress))},
        {NpgsqlDbType.MacAddr8, new NpgsqlTypeMapping(NpgsqlDbType.MacAddr8, DbType.Object, "macaddr8")},

        // Full-text search types
        {NpgsqlDbType.TsQuery, new NpgsqlTypeMapping(NpgsqlDbType.TsQuery, DbType.Object, "tsquery",
            typeof(NpgsqlTsQuery), typeof(NpgsqlTsQueryAnd), typeof(NpgsqlTsQueryEmpty),
            typeof(NpgsqlTsQueryFollowedBy),
            typeof(NpgsqlTsQueryLexeme), typeof(NpgsqlTsQueryNot), typeof(NpgsqlTsQueryOr), typeof(NpgsqlTsQueryBinOp)
        )},
        {NpgsqlDbType.TsVector, new NpgsqlTypeMapping(NpgsqlDbType.TsVector, DbType.Object, "tsvector", typeof(NpgsqlTsVector))},

        // Geometry types
        {NpgsqlDbType.Box, new NpgsqlTypeMapping(NpgsqlDbType.Box, DbType.Object, "box", typeof(NpgsqlBox))},
        {NpgsqlDbType.Circle, new NpgsqlTypeMapping(NpgsqlDbType.Circle, DbType.Object, "circle", typeof(NpgsqlCircle))},
        {NpgsqlDbType.Line, new NpgsqlTypeMapping(NpgsqlDbType.Line, DbType.Object, "line", typeof(NpgsqlLine))},
        {NpgsqlDbType.LSeg, new NpgsqlTypeMapping(NpgsqlDbType.LSeg, DbType.Object, "lseg", typeof(NpgsqlLSeg))},
        {NpgsqlDbType.Path, new NpgsqlTypeMapping(NpgsqlDbType.Path, DbType.Object, "path", typeof(NpgsqlPath))},
        {NpgsqlDbType.Point, new NpgsqlTypeMapping(NpgsqlDbType.Point, DbType.Object, "point", typeof(NpgsqlPoint))},
        {NpgsqlDbType.Polygon, new NpgsqlTypeMapping(NpgsqlDbType.Polygon, DbType.Object, "polygon", typeof(NpgsqlPolygon))},
        {NpgsqlDbType.Geometry, new NpgsqlTypeMapping(NpgsqlDbType.Geometry, DbType.Object, "geometry", typeof(Geometry))},

        // LTree types
        {NpgsqlDbType.LQuery, new NpgsqlTypeMapping(NpgsqlDbType.LQuery, DbType.Object, "lquery")},
        {NpgsqlDbType.LTree, new NpgsqlTypeMapping(NpgsqlDbType.LTree, DbType.Object, "ltree")},
        {NpgsqlDbType.LTxtQuery, new NpgsqlTypeMapping(NpgsqlDbType.LTxtQuery, DbType.Object, "ltxtquery")},

        // UInt types
        {NpgsqlDbType.Oid, new NpgsqlTypeMapping(NpgsqlDbType.Oid, DbType.Object, "oid")},
        {NpgsqlDbType.Xid, new NpgsqlTypeMapping(NpgsqlDbType.Xid, DbType.Object, "xid")},
        {NpgsqlDbType.Xid8, new NpgsqlTypeMapping(NpgsqlDbType.Xid8, DbType.Object, "xid8")},
        {NpgsqlDbType.Cid, new NpgsqlTypeMapping(NpgsqlDbType.Cid, DbType.Object, "cid")},
        {NpgsqlDbType.Regtype, new NpgsqlTypeMapping(NpgsqlDbType.Regtype, DbType.Object, "regtype")},
        {NpgsqlDbType.Regconfig, new NpgsqlTypeMapping(NpgsqlDbType.Regconfig, DbType.Object, "regconfig")},

        // Misc types
        {NpgsqlDbType.Boolean, new NpgsqlTypeMapping(NpgsqlDbType.Boolean, DbType.Boolean, "boolean", typeof(bool))},
        {NpgsqlDbType.Bytea, new NpgsqlTypeMapping(NpgsqlDbType.Bytea, DbType.Binary, "bytea", typeof(byte[]), typeof(ArraySegment<byte>),
            typeof(ReadOnlyMemory<byte>), typeof(Memory<byte>))},
        {NpgsqlDbType.Uuid, new NpgsqlTypeMapping(NpgsqlDbType.Uuid, DbType.Guid, "uuid", typeof(Guid))},
        {NpgsqlDbType.Varbit, new NpgsqlTypeMapping(NpgsqlDbType.Varbit, DbType.Object, "bit varying", typeof(BitArray), typeof(BitVector32))},
        {NpgsqlDbType.Bit, new NpgsqlTypeMapping(NpgsqlDbType.Bit, DbType.Object, "bit")},
        {NpgsqlDbType.Hstore, new NpgsqlTypeMapping(NpgsqlDbType.Hstore, DbType.Object, "hstore", typeof(Dictionary<string, string?>),
            typeof(IDictionary<string, string?>), typeof(ImmutableDictionary<string, string?>))},

        // Internal types
        {NpgsqlDbType.Int2Vector, new NpgsqlTypeMapping(NpgsqlDbType.Int2Vector, DbType.Object, "int2vector")},
        {NpgsqlDbType.Oidvector, new NpgsqlTypeMapping(NpgsqlDbType.Oidvector, DbType.Object, "oidvector")},
        {NpgsqlDbType.PgLsn, new NpgsqlTypeMapping(NpgsqlDbType.PgLsn, DbType.Object, "pg_lsn", typeof(NpgsqlLogSequenceNumber))},
        {NpgsqlDbType.Tid, new NpgsqlTypeMapping(NpgsqlDbType.Tid, DbType.Object, "tid", typeof(NpgsqlTid))},
        {NpgsqlDbType.InternalChar, new NpgsqlTypeMapping(NpgsqlDbType.InternalChar, DbType.Object, "char")},

        // Special types
        {NpgsqlDbType.Unknown, new NpgsqlTypeMapping(NpgsqlDbType.Unknown, DbType.Object, "unknown")}
    });
}
