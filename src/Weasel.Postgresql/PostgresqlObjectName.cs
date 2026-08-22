using Weasel.Core;

namespace Weasel.Postgresql;

/// <remarks>
///     Like <see cref="DbObjectName" />, this type does not validate. <see cref="From" /> is sometimes
///     assumed to be a sanitizing boundary and is not -- it quotes for rendering, which is not the same as
///     checking that a name is safe. Use <see cref="PostgresqlMigrator.AssertValidIdentifier" /> for that
///     (weasel#416).
/// </remarks>
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

    /// <summary>
    ///     A name can arrive already delimited -- <c>QualifiedNameParser</c> keeps the parts of a
    ///     qualified name exactly as written, and Weasel emitted most identifiers bare until 9.25, so
    ///     delimiting one by hand was the only way to use it. The model has to hold the spelling the
    ///     catalog reports, because that is what introspection binds; holding the delimited spelling
    ///     matched nothing, so the object read as absent and was recreated on every run (weasel#499).
    /// </summary>
    public PostgresqlObjectName(string schema, string name, SchemaUtils.IdentifierUsage usage)
        : base(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name),
            PostgresqlProvider.Instance.ToQualifiedName(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name)))
    {
        _usage = usage;
    }

    [Obsolete("Use the constructor with IdentifierUsage parameter. This overload will be removed in a future version.")]
    public PostgresqlObjectName(string schema, string name, string qualifiedName)
        : this(schema, name, qualifiedName, SchemaUtils.IdentifierUsage.General)
    {
    }

    /// <remarks>
    ///     The schema and name are normalized for the same reason as the constructor above; the
    ///     qualified name is taken as the caller gave it, since it is already the rendered form.
    /// </remarks>
    public PostgresqlObjectName(string schema, string name, string qualifiedName,
        SchemaUtils.IdentifierUsage usage)
        : base(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name), qualifiedName)
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
