using System.Data;
using JasperFx.Core;
using JasperFx.Core.Reflection;

namespace Weasel.Core;

/// <summary>
/// Static lookup of .NET type to DbType
/// </summary>
public static class DbTypeMapper
{
    private static ImHashMap<Type, DbType> _types = ImHashMap<Type, DbType>.Empty;

    static DbTypeMapper()
    {


        _types = _types.AddOrUpdate(typeof(byte), DbType.Byte);
        _types = _types.AddOrUpdate(typeof(sbyte),DbType.SByte);
        _types = _types.AddOrUpdate(typeof(short),DbType.Int16);
        _types = _types.AddOrUpdate(typeof(ushort),DbType.UInt16);
        _types = _types.AddOrUpdate(typeof(int),DbType.Int32);
        _types = _types.AddOrUpdate(typeof(uint),DbType.UInt32);
        _types = _types.AddOrUpdate(typeof(long),DbType.Int64);
        _types = _types.AddOrUpdate(typeof(ulong),DbType.UInt64);
        _types = _types.AddOrUpdate(typeof(float),DbType.Single);
        _types = _types.AddOrUpdate(typeof(double),DbType.Double);
        _types = _types.AddOrUpdate(typeof(decimal),DbType.Decimal);
        _types = _types.AddOrUpdate(typeof(bool),DbType.Boolean);
        _types = _types.AddOrUpdate(typeof(string),DbType.String);
        _types = _types.AddOrUpdate(typeof(char),DbType.StringFixedLength);
        _types = _types.AddOrUpdate(typeof(Guid),DbType.Guid);
        _types = _types.AddOrUpdate(typeof(DateTime),DbType.DateTime);
        _types = _types.AddOrUpdate(typeof(DateTimeOffset),DbType.DateTimeOffset);
        _types = _types.AddOrUpdate(typeof(TimeSpan),DbType.Time);
        _types = _types.AddOrUpdate(typeof(byte[]), DbType.Binary);
    }

    /// <summary>
    /// Find the DbType for a .NET Type
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static DbType? Lookup(Type type)
    {
        if (_types.TryFind(type, out var dbType)) return dbType;

        if (type.IsNullable())
        {
            var raw = type.GetInnerTypeFromNullable();
            if (_types.TryFind(raw, out dbType))
            {
                _types = _types.AddOrUpdate(type, dbType);
                return dbType;
            }
        }

        return default;
    }
}
