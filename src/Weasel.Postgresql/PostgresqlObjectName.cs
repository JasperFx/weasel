using Weasel.Core;

namespace Weasel.Postgresql;

public class PostgresqlObjectName: DbObjectName
{
    public PostgresqlObjectName(string schema, string name)
        : base(schema, name, PostgresqlProvider.Instance.ToQualifiedName(schema, name))
    {
    }

    public PostgresqlObjectName(string schema, string name, string qualifiedName)
        : base(schema, name, qualifiedName)
    {
    }

    private PostgresqlObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static PostgresqlObjectName From(DbObjectName dbObjectName) =>
        new PostgresqlObjectName(dbObjectName);

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
