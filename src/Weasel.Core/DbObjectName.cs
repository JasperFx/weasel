namespace Weasel.Core;

/// <summary>
///     Models a database object with both schema name and object name
/// </summary>
public class DbObjectName
{
    public DbObjectName(string schema, string name)
    {
        Schema = schema;
        Name = name;
        QualifiedName = $"{Schema}.{Name}";
    }

    public string Schema { get; }
    public string Name { get; }
    public string QualifiedName { get; }

    public DbObjectName ToTempCopyTable()
    {
        return new DbObjectName(Schema, Name + "_temp");
    }

    public static DbObjectName Parse(IDatabaseProvider provider, string qualifiedName)
    {
        var parts = ParseQualifiedName(provider, qualifiedName);
        return new DbObjectName(parts[0], parts[1]);
    }

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

    protected static string[] ParseQualifiedName(IDatabaseProvider provider, string qualifiedName)
    {
        var parts = qualifiedName.Split('.');
        if (parts.Length == 1)
        {
            return new[] { provider.DefaultDatabaseSchemaName, qualifiedName };
        }

        if (parts.Length != 2)
        {
            throw new InvalidOperationException(
                $"Could not parse QualifiedName: '{qualifiedName}'. Number or parts should be 2s but is {parts.Length}");
        }

        return parts;
    }
}
