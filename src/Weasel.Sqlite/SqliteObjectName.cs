using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Sqlite;

public class SqliteObjectName: DbObjectName
{
    protected override string QuotedQualifiedName => $"{SchemaUtils.QuoteName(Schema)}.{SchemaUtils.QuoteName(Name)}";

    public SqliteObjectName(string schema, string name)
        : base(schema, name, SqliteProvider.Instance.As<IDatabaseProvider>().ToQualifiedName(schema, name))
    {
    }

    private SqliteObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static SqliteObjectName From(DbObjectName dbObjectName) =>
        new SqliteObjectName(dbObjectName);

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
