using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Sqlite;

public class SqliteObjectName: DbObjectName
{
    protected override string QuotedQualifiedName =>
        Schema.Equals("main", StringComparison.OrdinalIgnoreCase)
            ? SchemaUtils.QuoteName(Name)
            : $"{SchemaUtils.QuoteName(Schema)}.{SchemaUtils.QuoteName(Name)}";

    /// <summary>
    ///     A name can arrive already delimited -- <c>QualifiedNameParser</c> keeps the parts of a
    ///     qualified name exactly as written, and Weasel emitted most identifiers bare until 9.25, so
    ///     delimiting one by hand was the only way to use it. The model has to hold the spelling the
    ///     catalog reports, because that is what introspection binds; holding the delimited spelling
    ///     matched nothing, so the object read as absent and was recreated on every run (weasel#499).
    /// </summary>
    public SqliteObjectName(string schema, string name)
        : base(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name),
            BuildQualifiedName(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name)))
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
