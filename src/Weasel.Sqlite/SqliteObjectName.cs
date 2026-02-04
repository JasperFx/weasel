using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Sqlite;

public class SqliteObjectName: DbObjectName
{
    protected override string QuotedQualifiedName =>
        Schema.Equals("main", StringComparison.OrdinalIgnoreCase)
            ? SchemaUtils.QuoteName(Name)
            : $"{SchemaUtils.QuoteName(Schema)}.{SchemaUtils.QuoteName(Name)}";

    public SqliteObjectName(string schema, string name)
        : base(schema, name, BuildQualifiedName(schema, name))
    {
    }

    private static string BuildQualifiedName(string schema, string name)
    {
        return schema.Equals("main", StringComparison.OrdinalIgnoreCase)
            ? SchemaUtils.QuoteName(name)
            : $"{SchemaUtils.QuoteName(schema)}.{SchemaUtils.QuoteName(name)}";
    }

    public SqliteObjectName(string name)
        : this("main", name)
    {
    }

    private SqliteObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static SqliteObjectName From(DbObjectName dbObjectName) => new(dbObjectName);

    private new bool Equals(DbObjectName other)
    {
        // SQLite is case-insensitive for identifiers by default (unless quoted)
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
            // Use case-insensitive hash since SQLite is case-insensitive
            return (typeof(DbObjectName).GetHashCode() * 397) ^ QualifiedName.ToLowerInvariant().GetHashCode();
        }
    }
}
