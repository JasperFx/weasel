using Weasel.Core;

namespace Weasel.Postgresql;

public class PostgresqlObjectName: DbObjectName
{
    private readonly SchemaUtils.IdentifierUsage _usage;

    protected override string QuotedQualifiedName =>
        $"{SchemaUtils.QuoteName(Schema, _usage)}.{SchemaUtils.QuoteName(Name, _usage)}";

    [Obsolete("Use the constructor with IdentifierUsage parameter. This overload will be removed in a future version.")]
    public PostgresqlObjectName(string schema, string name)
        : this(schema, name, SchemaUtils.IdentifierUsage.General)
    {
    }

    public PostgresqlObjectName(string schema, string name, SchemaUtils.IdentifierUsage usage)
        : base(schema, name, PostgresqlProvider.Instance.ToQualifiedName(schema, name))
    {
        _usage = usage;
    }

    [Obsolete("Use the constructor with IdentifierUsage parameter. This overload will be removed in a future version.")]
    public PostgresqlObjectName(string schema, string name, string qualifiedName)
        : this(schema, name, qualifiedName, SchemaUtils.IdentifierUsage.General)
    {
    }

    public PostgresqlObjectName(string schema, string name, string qualifiedName,
        SchemaUtils.IdentifierUsage usage)
        : base(schema, name, qualifiedName)
    {
        _usage = usage;
    }

    private PostgresqlObjectName(DbObjectName dbObjectName, SchemaUtils.IdentifierUsage usage)
        : this(dbObjectName.Schema, dbObjectName.Name, usage)
    {
    }

    [Obsolete("Use From(DbObjectName, IdentifierUsage). This overload will be removed in a future version.")]
    public static PostgresqlObjectName From(DbObjectName dbObjectName) =>
        From(dbObjectName, SchemaUtils.IdentifierUsage.General);

    public static PostgresqlObjectName From(DbObjectName dbObjectName,
        SchemaUtils.IdentifierUsage usage)
    {
        var schema = dbObjectName.Schema;
        var name = dbObjectName.Name;
        var qualifiedName =
            $"{SchemaUtils.QuoteName(schema, usage)}.{SchemaUtils.QuoteName(name, usage)}";

        return new PostgresqlObjectName(schema, name, qualifiedName, usage);
    }

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
