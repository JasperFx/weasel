using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.Specialized;
using System.Data;
using System.Net;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Text.Json;
using NpgsqlTypes;

namespace Weasel.Postgresql
{
    public class NpgsqlTypeMapping
    {
        public NpgsqlDbType? NpgsqlDbType { get; }
        public DbType DbType { get; }
        public string? DataTypeName { get; }
        public Type[] ClrTypes { get; }

        public NpgsqlTypeMapping(NpgsqlDbType? npgsqlDbType, DbType dbType, string? dataTypeName, Type clrType)
            => (NpgsqlDbType, DbType, DataTypeName, ClrTypes) = (npgsqlDbType, dbType, dataTypeName, new[] { clrType });

        public NpgsqlTypeMapping(NpgsqlDbType? npgsqlDbType, DbType dbType, string? dataTypeName, params Type[] clrTypes)
            => (NpgsqlDbType, DbType, DataTypeName, ClrTypes) = (npgsqlDbType, dbType, dataTypeName, clrTypes);
    }

    /// <summary>
    /// Class defining custom NpgsqlType <=> DbType <=> CLR types
    /// </summary>
    /// <remarks>Based on https://github.com/npgsql/npgsql/blob/a1d366c5692cc00a5edc1ec5bc9090952c7a63e7/src/Npgsql/TypeMapping/BuiltInTypeHandlerResolver.cs</remarks>
    public class NpgsqlTypeMapper
    {
        public static readonly List<NpgsqlTypeMapping> Mappings = new()
        {
            // Numeric types
            new(NpgsqlDbType.Smallint, DbType.Int16,   "smallint",         typeof(short), typeof(byte), typeof(sbyte)),
            new(NpgsqlDbType.Integer,  DbType.Int32,   "integer",          typeof(int)),
            new(NpgsqlDbType.Integer,  DbType.Int32,   "integer",          typeof(int)),
            new(NpgsqlDbType.Bigint,   DbType.Int64,   "bigint",           typeof(long)),
            new(NpgsqlDbType.Real,     DbType.Single,  "real",             typeof(float)),
            new(NpgsqlDbType.Double,   DbType.Double,  "double precision", typeof(double)),
            new(NpgsqlDbType.Numeric,  DbType.Decimal, "decimal",          typeof(decimal), typeof(BigInteger)),
            new(NpgsqlDbType.Numeric,  DbType.Decimal, "decimal",          typeof(decimal), typeof(BigInteger)),
            new(NpgsqlDbType.Money,    DbType.Int16,   "money"),

            // Text types
            new(NpgsqlDbType.Text,      DbType.String, "text", typeof(string), typeof(char[]), typeof(char), typeof(ArraySegment<char>)),
            new(NpgsqlDbType.Xml,       DbType.Xml,    "xml"),
            new(NpgsqlDbType.Varchar,   DbType.String, "character varying"),
            new(NpgsqlDbType.Varchar,   DbType.String, "character varying"),
            new(NpgsqlDbType.Char,      DbType.String, "character"),
            new(NpgsqlDbType.Name,      DbType.String, "name"),
            new(NpgsqlDbType.Refcursor, DbType.String, "refcursor"),
            new(NpgsqlDbType.Citext,    DbType.String, "citext"),
            new(NpgsqlDbType.Jsonb,     DbType.Object, "jsonb", typeof(JsonDocument)),
            new(NpgsqlDbType.Json,      DbType.Object, "json"),
            new(NpgsqlDbType.JsonPath,  DbType.Object, "jsonpath"),

            // Date/time types
#pragma warning disable 618 // NpgsqlDateTime is obsolete, remove in 7.0
            new(NpgsqlDbType.Timestamp,   DbType.DateTime,       "timestamp without time zone", typeof(DateTime), typeof(NpgsqlDateTime)),
#pragma warning disable 618
            new(NpgsqlDbType.TimestampTz, DbType.DateTimeOffset, "timestamp with time zone",    typeof(DateTimeOffset)),
            new(NpgsqlDbType.Date,        DbType.Date,           "date",                        typeof(NpgsqlDate)
#if NET6_0_OR_GREATER
                , typeof(DateOnly)
#endif
            ),
            new(NpgsqlDbType.Time,        DbType.Time,     "time without time zone"
#if NET6_0_OR_GREATER
                , typeof(TimeOnly)
#endif
            ),
            new(NpgsqlDbType.TimeTz,      DbType.Object,   "time with time zone"),
            new(NpgsqlDbType.Interval,    DbType.Object,   "interval", typeof(TimeSpan), typeof(NpgsqlTimeSpan)),

            new(NpgsqlDbType.Array | NpgsqlDbType.Timestamp,   DbType.Object, "timestamp without time zone[]"),
            new(NpgsqlDbType.Array | NpgsqlDbType.TimestampTz, DbType.Object, "timestamp with time zone[]"),
            new(NpgsqlDbType.Range | NpgsqlDbType.Timestamp,   DbType.Object, "tsrange"),
            new(NpgsqlDbType.Range | NpgsqlDbType.TimestampTz, DbType.Object, "tstzrange"),
            new(NpgsqlDbType.Multirange | NpgsqlDbType.Timestamp,   DbType.Object, "tsmultirange"),
            new(NpgsqlDbType.Multirange | NpgsqlDbType.TimestampTz, DbType.Object, "tstzmultirange"),

            // Network types
            new(NpgsqlDbType.Cidr,     DbType.Object, "cidr"),
#pragma warning disable 618
            new(NpgsqlDbType.Inet,     DbType.Object, "inet", typeof(IPAddress), typeof((IPAddress Address, int Subnet)), typeof(NpgsqlInet), IPAddress.Loopback.GetType()),
#pragma warning restore 618
            new(NpgsqlDbType.MacAddr,  DbType.Object, "macaddr", typeof(PhysicalAddress)),
            new(NpgsqlDbType.MacAddr8, DbType.Object, "macaddr8"),

            // Full-text search types
            new(NpgsqlDbType.TsQuery,  DbType.Object, "tsquery",
                typeof(NpgsqlTsQuery), typeof(NpgsqlTsQueryAnd), typeof(NpgsqlTsQueryEmpty), typeof(NpgsqlTsQueryFollowedBy),
                typeof(NpgsqlTsQueryLexeme), typeof(NpgsqlTsQueryNot), typeof(NpgsqlTsQueryOr), typeof(NpgsqlTsQueryBinOp)
                ),
            new(NpgsqlDbType.TsVector, DbType.Object, "tsvector", typeof(NpgsqlTsVector)),

            // Geometry types
            new(NpgsqlDbType.Box,     DbType.Object, "box",     typeof(NpgsqlBox)),
            new(NpgsqlDbType.Circle,  DbType.Object, "circle",  typeof(NpgsqlCircle)),
            new(NpgsqlDbType.Line,    DbType.Object, "line",    typeof(NpgsqlLine)),
            new(NpgsqlDbType.LSeg,    DbType.Object, "lseg",    typeof(NpgsqlLSeg)),
            new(NpgsqlDbType.Path,    DbType.Object, "path",    typeof(NpgsqlPath)),
            new(NpgsqlDbType.Point,   DbType.Object, "point",   typeof(NpgsqlPoint)),
            new(NpgsqlDbType.Polygon, DbType.Object, "polygon", typeof(NpgsqlPolygon)),

            // LTree types
            new(NpgsqlDbType.LQuery,    DbType.Object, "lquery"),
            new(NpgsqlDbType.LTree,     DbType.Object, "ltree"),
            new(NpgsqlDbType.LTxtQuery, DbType.Object, "ltxtquery"),

            // UInt types
            new(NpgsqlDbType.Oid,       DbType.Object, "oid"),
            new(NpgsqlDbType.Xid,       DbType.Object, "xid"),
            new(NpgsqlDbType.Xid8,      DbType.Object, "xid8"),
            new(NpgsqlDbType.Cid,       DbType.Object, "cid"),
            new(NpgsqlDbType.Regtype,   DbType.Object, "regtype"),
            new(NpgsqlDbType.Regconfig, DbType.Object, "regconfig"),

            // Misc types
            new(NpgsqlDbType.Boolean, DbType.Boolean, "boolean", typeof(bool)),
            new(NpgsqlDbType.Boolean, DbType.Boolean, "boolean", typeof(bool)),
            new(NpgsqlDbType.Bytea,   DbType.Binary,  "bytea", typeof(byte[]), typeof(ArraySegment<byte>)
#if !NETSTANDARD2_0
                , typeof(ReadOnlyMemory<byte>), typeof(Memory<byte>)
#endif
            ),
            new(NpgsqlDbType.Uuid,    DbType.Guid,    "uuid", typeof(Guid)),
            new(NpgsqlDbType.Varbit,  DbType.Object,  "bit varying", typeof(BitArray), typeof(BitVector32)),
            new(NpgsqlDbType.Varbit,  DbType.Object,  "bit varying", typeof(BitArray), typeof(BitVector32)),
            new(NpgsqlDbType.Bit,     DbType.Object,  "bit"),
            new(NpgsqlDbType.Hstore,  DbType.Object,  "hstore", typeof(Dictionary<string, string?>), typeof(IDictionary<string, string?>)
#if !NETSTANDARD2_0 && !NETSTANDARD2_1
                , typeof(ImmutableDictionary<string, string?>)
#endif
            ),

            // Internal types
            new(NpgsqlDbType.Int2Vector,   DbType.Object, "int2vector"),
            new(NpgsqlDbType.Oidvector,    DbType.Object, "oidvector"),
            new(NpgsqlDbType.PgLsn,        DbType.Object, "pg_lsn", typeof(NpgsqlLogSequenceNumber)),
            new(NpgsqlDbType.Tid,          DbType.Object, "tid", typeof(NpgsqlTid)),
            new(NpgsqlDbType.InternalChar, DbType.Object, "char"),

            // Special types
            new(NpgsqlDbType.Unknown, DbType.Object, "unknown"),
        };
    }
}
