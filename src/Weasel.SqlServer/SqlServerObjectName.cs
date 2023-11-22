using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.SqlServer;

public class SqlServerObjectName: DbObjectName
{
    public SqlServerObjectName(string schema, string name)
        : base(schema, name, SqlServerProvider.Instance.As<IDatabaseProvider>().ToQualifiedName(schema, name))
    {
    }

    private SqlServerObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static SqlServerObjectName From(DbObjectName dbObjectName) =>
        new SqlServerObjectName(dbObjectName);

    private new bool Equals(DbObjectName other)
    {
        return string.Equals(QualifiedName, other.QualifiedName, StringComparison.OrdinalIgnoreCase);
    }

    public override bool Equals(object? obj)
    {
        if (obj is DbObjectName dbObjectName)
        {
            return Equals(dbObjectName);
        }

        return base.Equals(obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (typeof(DbObjectName).GetHashCode() * 397) ^ QualifiedName.GetHashCode();
        }
    }
}
