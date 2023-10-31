namespace Weasel.Core;

/// <summary>
///     Models a database object with both schema name and object name
/// </summary>
public class DbObjectName
{
    [Obsolete("Use PostgresqlObjectName, SqlServerObjectName, or Parse method with IDatabaseProvider instead.")]
    public DbObjectName(string schema, string name): this(schema, name, $"{schema}.{name}")
    {
        Schema = schema;
        Name = name;
    }

    protected DbObjectName(string schema, string name, string qualifiedName)
    {
        Schema = schema;
        Name = name;
        QualifiedName = qualifiedName;
    }

    public string Schema { get; }
    public string Name { get; }
    public string QualifiedName { get; }

    public DbObjectName ToTempCopyTable()
    {
        return new DbObjectName(Schema, Name + "_temp");
    }

    [Obsolete("Use method from database provider")]
    public static DbObjectName Parse(IDatabaseProvider provider, string qualifiedName) =>
        provider.Parse(qualifiedName);

    [Obsolete("Use method from database provider")]
    public static DbObjectName Parse(IDatabaseProvider provider, string schemaName, string objectName) =>
        provider.Parse(schemaName, objectName);

    public override string ToString()
    {
        return QualifiedName;
    }

    protected bool Equals(DbObjectName other)
    {
        return GetType() == other.GetType() &&
               string.Equals(QualifiedName, other.QualifiedName, StringComparison.OrdinalIgnoreCase);
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj.GetType() != GetType())
        {
            return false;
        }

        return Equals((DbObjectName)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (GetType().GetHashCode() * 397) ^ (QualifiedName?.GetHashCode() ?? 0);
        }
    }
}
