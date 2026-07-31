namespace Weasel.Core;

/// <summary>
///     Models a database object with both schema name and object name
/// </summary>
/// <remarks>
///     This type does no validation whatsoever, and neither do its provider-specific subclasses. It is a
///     value holder, not a sanitizing boundary -- constructing one, or going through a <c>Parse</c> or
///     <c>From</c> factory, does not make a name safe to interpolate into DDL. Identifier validation lives
///     in <c>Migrator.AssertValidIdentifier</c>, which the migration path applies to
///     <see cref="Name" /> (weasel#416).
/// </remarks>
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

    protected virtual string QuotedQualifiedName => QualifiedName;

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
        return QuotedQualifiedName;
    }

    protected bool Equals(DbObjectName other)
    {
        return string.Equals(QualifiedName, other.QualifiedName, StringComparison.OrdinalIgnoreCase);
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

        if (obj is not DbObjectName name)
        {
            return false;
        }

        return Equals(name);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (GetType().GetHashCode() * 397) ^ (QualifiedName?.GetHashCode() ?? 0);
        }
    }
}
